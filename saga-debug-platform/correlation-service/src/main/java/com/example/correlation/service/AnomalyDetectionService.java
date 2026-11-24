package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.execution.ExecutionProjection;
import com.example.correlation.execution.EvidenceTimeline;
import com.example.correlation.execution.ExecutionProjectionService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AnomalyDetectionService {
    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionService.class);

    private static final List<SagaStatus> RUNNING_STATUSES =
            List.of(SagaStatus.STARTED, SagaStatus.IN_PROGRESS, SagaStatus.COMPENSATING);
    private static final List<SagaStatus> IN_FLIGHT_STATUSES =
            List.of(SagaStatus.IN_PROGRESS, SagaStatus.COMPENSATING);
    private static final List<SagaStatus> SETTLED_STATUSES =
            List.of(SagaStatus.COMPLETED, SagaStatus.COMPENSATED);
    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final WorkflowRegistry workflowRegistry;
    private final ObjectMapper objectMapper;
    private final DetectionProperties detectionProperties;
    private final ExecutionProjectionService executionProjectionService;

    public AnomalyDetectionService(
            SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            WorkflowRegistry workflowRegistry,
            ObjectMapper objectMapper,
            DetectionProperties detectionProperties,
            ExecutionProjectionService executionProjectionService) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.workflowRegistry = workflowRegistry;
        this.objectMapper = objectMapper;
        this.detectionProperties = detectionProperties;
        this.executionProjectionService = executionProjectionService;
    }

    public void detectOutOfOrder(String sagaId, List<SagaEvent> sagaEvents) {
        if (sagaEvents.size() < 2) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.OUT_OF_ORDER)) return;

        Map<String, List<SagaEvent>> byPartition = new LinkedHashMap<>();
        sagaEvents.stream()
                .sorted(Comparator.comparing(SagaEvent::getReceivedAt,
                        Comparator.nullsLast(Comparator.naturalOrder())))
                .forEach(e -> byPartition.computeIfAbsent(partitionOf(e.getTransportRef()),
                        key -> new ArrayList<>()).add(e));
        for (List<SagaEvent> events : byPartition.values()) {
            if (detectOutOfOrderWithin(sagaId, events)) return;
        }
    }

    static String partitionOf(String transportRef) {
        if (transportRef == null) return "";
        int offsetDash = transportRef.lastIndexOf('-');
        return offsetDash <= 0 ? "" : transportRef.substring(0, offsetDash);
    }

    private boolean detectOutOfOrderWithin(String sagaId, List<SagaEvent> events) {
        for (int i = 1; i < events.size(); i++) {
            SagaEvent previousEvent = events.get(i - 1);
            SagaEvent currentEvent = events.get(i);
            Instant prevCreated = previousEvent.getTimestamp();
            Instant currCreated = currentEvent.getTimestamp();
            Duration timeDiff = Duration.between(currCreated, prevCreated);

            Duration clockSkewTolerance = detectionProperties.getClockSkewTolerance();
            if (currCreated.isBefore(prevCreated) && timeDiff.compareTo(clockSkewTolerance) > 0) {
                createAnomaly(sagaId, AnomalyType.OUT_OF_ORDER,
                        String.format("Event '%s' arrived after '%s' but has earlier timestamp (%s < %s, gap: %dms)",
                                currentEvent.getEventType(),
                                previousEvent.getEventType(),
                                currCreated, prevCreated, timeDiff.toMillis()),
                        details()
                                .observedValue(String.format("%s timestamp %s before %s timestamp %s by %dms",
                                        currentEvent.getEventType(), currCreated,
                                        previousEvent.getEventType(), prevCreated,
                                        timeDiff.toMillis()))
                                .expectedValue(String.format("event timestamp >= previous timestamp minus %dms clock skew",
                                        clockSkewTolerance.toMillis()))
                                .thresholdSource("config:anomaly.detection.clock-skew-tolerance")
                                .evidenceEvents(previousEvent, currentEvent)
                                .measuredAt(currCreated));
                return true;
            }
        }
        return false;
    }

    public boolean detectInvalidTransition(WorkflowSpec workflow, String sagaId, String previousStep,
            String currentStep, String condition, String eventId, Instant eventTimestamp) {
        if (workflow == null || workflow.getTransitions() == null || previousStep == null) return false;

        WorkflowSpec.TransitionJudgment judgment =
                workflow.explainTransition(previousStep, currentStep, condition);
        if (judgment.verdict() != WorkflowSpec.TransitionVerdict.INVALID) {
            if (!judgment.stepsInBetween().isEmpty()) {
                log.debug("Saga {}: {} -> {} on {} is not adjacent, but {} lies between them "
                                + "in {} and has not been observed. Nothing concluded.",
                        sagaId, previousStep, currentStep, condition,
                        judgment.stepsInBetween(), workflow.getName());
            }
            return false;
        }

        AnomalyDetails evidence = details()
                .observedValue(handoff(previousStep, currentStep, condition))
                .evidenceEventIds(eventId);
        if (isInvalidTransitionStanding(sagaId, evidence)) return true;

        String expected = allowedTransitionsFrom(workflow, previousStep);
        createAnomaly(sagaId, AnomalyType.INVALID_TRANSITION,
                String.format("Invalid transition: %s -> %s (condition: %s). Expected from %s: [%s]",
                        previousStep, currentStep, condition, previousStep,
                        expected.isEmpty() ? "none defined" : expected),
                evidence
                        .expectedValue(expected.isEmpty() ? "no transition defined from " + previousStep : expected)
                        .thresholdSource("workflow:" + workflow.getName() + ".transitions")
                        .measuredAt(eventTimestamp));
        return true;
    }

    private boolean isInvalidTransitionStanding(String sagaId, AnomalyDetails evidence) {
        return anomalyRepository.findStandingBySagaId(sagaId).stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.INVALID_TRANSITION
                        && evidence.observedValue.equals(a.getObservedValue())
                        && Objects.equals(a.getEvidenceEventIds(), evidence.evidenceEventIds));
    }

    private static String allowedTransitionsFrom(WorkflowSpec workflow, String step) {
        return workflow.getTransitions().stream()
                .filter(t -> t.getFrom().equals(step))
                .map(t -> t.getTo() + " (on: " + t.getCondition() + ")")
                .collect(Collectors.joining(", "));
    }

    public void withdrawUnjudgedTransitions(String sagaId, Set<String> stillInvalid) {
        Instant now = Instant.now();
        for (Anomaly anomaly : anomalyRepository.findStandingBySagaId(sagaId)) {
            if (anomaly.getAnomalyType() != AnomalyType.INVALID_TRANSITION) continue;
            List<String> cited = anomaly.getEvidenceEventIds();
            String key = transitionKey(anomaly.getObservedValue(),
                    cited == null || cited.isEmpty() ? null : cited.get(0));
            if (stillInvalid.contains(key)) continue;
            anomaly.setRetractedAt(now);
            anomaly.setRetractionReason("the observations it paired are no longer adjacent "
                    + "once the rest of the run is in hand");
            anomalyRepository.save(anomaly);
            log.info("Withdrew INVALID_TRANSITION {} against execution {}: the full timeline "
                    + "does not contain that handoff", anomaly.getObservedValue(), sagaId);
        }
    }

    public static String transitionKey(String observedHandoff, String eventId) {
        return observedHandoff + "|" + eventId;
    }

    public static String handoff(String previousStep, String currentStep, String condition) {
        return String.format("%s -> %s on %s", previousStep, currentStep, condition);
    }

    public void detectRetryBurst(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        int burstThreshold = detectionProperties.getRetryBurstThreshold();
        Duration burstWindow = detectionProperties.getRetryBurstWindow();
        if (events.size() < burstThreshold) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.RETRY_BURST)) return;

        Set<String> retryEventTypes = workflow != null ? workflow.retryEventTypes() : Set.of();
        if (retryEventTypes.isEmpty()) return;

        List<SagaEvent> retryEvents = events.stream()
                .filter(e -> retryEventTypes.contains(e.getEventType()))
                .collect(Collectors.toList());
        if (retryEvents.size() < burstThreshold) return;

        for (int i = 0; i <= retryEvents.size() - burstThreshold; i++) {
            SagaEvent first = retryEvents.get(i);
            SagaEvent last = retryEvents.get(i + burstThreshold - 1);

            Duration gap = Duration.between(first.getTimestamp(), last.getTimestamp());
            if (gap.compareTo(burstWindow) <= 0) {
                List<SagaEvent> burstEvents = retryEvents.subList(i, i + burstThreshold);
                createAnomaly(sagaId, AnomalyType.RETRY_BURST,
                        String.format("%d retry events in %ds — possible retry storm%s",
                                burstThreshold, gap.getSeconds(),
                                retryPolicyNote(workflow, first.getEventType(), retryEvents.size())),
                        details()
                                .observedValue(String.format("%d retry events in %dms%s",
                                        burstEvents.size(), gap.toMillis(),
                                        retryPolicyNote(workflow, first.getEventType(), retryEvents.size())))
                                .expectedValue(String.format("fewer than %d retry events within %ds",
                                        burstThreshold, burstWindow.toSeconds()))
                                .thresholdSource("config:anomaly.detection.retry-burst")
                                .evidenceEvents(burstEvents)
                                .measuredAt(last.getTimestamp()));
                return;
            }
        }
    }

    private String retryPolicyNote(WorkflowSpec workflow, String retryEventType, int observedRetries) {
        if (workflow == null) return "";

        Optional<Integer> declared = workflow.classifyEvent(retryEventType)
                .map(WorkflowSpec.EventRole::stepId)
                .flatMap(workflow::declaredMaxAttempts);
        if (declared.isEmpty()) return "";

        int allowedRetries = Math.max(0, declared.get() - 1);
        return String.format(" [observed %d retries; declared policy allows %d — %s policy]",
                observedRetries, allowedRetries,
                observedRetries > allowedRetries ? "exceeds" : "within");
    }

    public void detectDuplicateEvent(String sagaId, String eventId, String eventType) {
        detectDuplicateEvent(sagaId, eventId, eventType, null, null);
    }

    public void detectDuplicateEvent(String sagaId, String eventId, String eventType,
            String storedOn, String arrivedOn) {
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.EXACT_DUPLICATE_EVENT)) return;

        boolean recordsKnown = storedOn != null && arrivedOn != null;
        String observed = recordsKnown
                ? String.format("eventId=%s, eventType=%s, delivered on %s and again on %s",
                        eventId, eventType, storedOn, arrivedOn)
                : String.format("eventId=%s, eventType=%s", eventId, eventType);

        createAnomaly(sagaId, AnomalyType.EXACT_DUPLICATE_EVENT,
                String.format("Duplicate event detected: eventId=%s, type=%s", eventId, eventType),
                details()
                        .observedValue(observed)
                        .expectedValue("event_id appears once per saga event stream")
                        .thresholdSource(recordsKnown
                                ? "db:saga_events.transport_ref"
                                : "db:saga_events.event_id")
                        .evidenceEventIds(eventId)
                        .measuredAt(Instant.now()));
    }

    public void detectStateRegression(String sagaId, EvidenceTimeline.Regression regression) {
        if (regression == null) return;

        SagaStatus from = regression.from();
        SagaStatus to = regression.to();
        raiseOrRevise(sagaId, AnomalyType.STATE_REGRESSION,
                String.format("State regression: %s → %s (rank %d → %d) between '%s' and '%s'",
                        from, to, from.progressRank(), to.progressRank(),
                        regression.earlier().getEventType(), regression.later().getEventType()),
                details()
                        .observedValue(String.format("%s -> %s", from, to))
                        .expectedValue("saga status rank is monotonic non-decreasing in event time")
                        .thresholdSource("code:SagaStatus.progressRank")
                        .evidenceEvents(regression.earlier(), regression.later())
                        .measuredAt(regression.later().getTimestamp()));
    }

    public void detectForbiddenPostTerminal(String sagaId, SagaStatus endedAs,
            SagaEvent terminalEvent, SagaEvent lateEvent) {
        if (endedAs == null || !endedAs.isTerminal() || terminalEvent == null || lateEvent == null) {
            return;
        }
        String description = String.format("Event '%s' at %s happened after '%s' ended the saga as %s at %s",
                lateEvent.getEventType(), lateEvent.getTimestamp(),
                terminalEvent.getEventType(), endedAs, terminalEvent.getTimestamp());
        AnomalyDetails evidence = details()
                .observedValue(String.format("%s after terminal status %s", lateEvent.getEventType(), endedAs))
                .expectedValue("no events after terminal saga status")
                .thresholdSource("code:terminal-status-guard")
                .evidenceEvents(terminalEvent, lateEvent)
                .measuredAt(lateEvent.getTimestamp());
        raiseOrRevise(sagaId, AnomalyType.FORBIDDEN_POST_TERMINAL, description, evidence);
    }

    private void raiseOrRevise(String sagaId, AnomalyType type, String description, AnomalyDetails evidence) {
        if (!anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, type)) {
            createAnomaly(sagaId, type, description, evidence);
            return;
        }
        for (Anomaly anomaly : anomalyRepository.findStandingBySagaId(sagaId)) {
            if (anomaly.getAnomalyType() != type) continue;
            if (Objects.equals(anomaly.getEvidenceEventIds(), evidence.evidenceEventIds)
                    && Objects.equals(anomaly.getMeasuredAt(), evidence.measuredAt)
                    && Objects.equals(anomaly.getDescription(), description)) continue;
            anomaly.setDescription(description);
            anomaly.setObservedValue(evidence.observedValue);
            anomaly.setExpectedValue(evidence.expectedValue);
            anomaly.setThresholdSource(evidence.thresholdSource);
            anomaly.setEvidenceEventIds(evidence.evidenceEventIds);
            anomaly.setMeasuredAt(evidence.measuredAt);
            anomaly.setDetectedAt(Instant.now());
            anomalyRepository.save(anomaly);
            log.info("Anomaly evidence revised: [{}] {} - {}", type, sagaId, description);
        }
    }

    public void detectIndeterminateOrder(String sagaId, List<SagaEvent> contested) {
        if (contested == null || contested.size() < 2) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.INDETERMINATE_ORDER)) return;

        String types = distinctTypes(contested);
        createAnomaly(sagaId, AnomalyType.INDETERMINATE_ORDER,
                String.format("Cannot determine which of [%s] happened last; they share an "
                                + "event time and the workflow declares no dependency between "
                                + "their steps. The saga's outcome is not stated.", types),
                details()
                        .observedValue(String.format("[%s] all at %s", types,
                                contested.get(0).getTimestamp()))
                        .expectedValue("event time or workflow dependency orders the run's "
                                + "outcome-bearing events")
                        .thresholdSource("code:EvidenceTimeline.businessOrder")
                        .evidenceEvents(contested)
                        .measuredAt(lastTimestamp(contested)));
    }

    public void detectMultiTerminalConflict(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);
        List<SagaEvent> succeeded = timeline.endingsAs(SagaStatus.COMPLETED);
        List<SagaEvent> notSucceeded = new ArrayList<>(timeline.endingsAs(SagaStatus.FAILED));
        notSucceeded.addAll(timeline.endingsAs(SagaStatus.COMPENSATED));

        if (succeeded.isEmpty() || notSucceeded.isEmpty()) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MULTI_TERMINAL_CONFLICT)
                    && anomalyRepository.retractClaims(sagaId, Set.of(AnomalyType.MULTI_TERMINAL_CONFLICT),
                            Instant.now(), "read against every branch the run has one outcome; the other "
                                    + "was a forward reply after compensation began") > 0) {
                log.info("Withdrew MULTI_TERMINAL_CONFLICT against execution {}: one outcome", sagaId);
            }
            return;
        }

        List<SagaEvent> terminalEvidence = new ArrayList<>(succeeded);
        terminalEvidence.addAll(notSucceeded);
        terminalEvidence.sort(Comparator.comparing(SagaEvent::getTimestamp));

        String successTypes = distinctTypes(succeeded);
        String failTypes = distinctTypes(notSucceeded);
        raiseOrRevise(sagaId, AnomalyType.MULTI_TERMINAL_CONFLICT,
                String.format("Both success [%s] and non-success [%s] terminal events present in same saga",
                        successTypes, failTypes),
                details()
                        .observedValue(String.format("success=[%s], non-success=[%s]", successTypes, failTypes))
                        .expectedValue("only one terminal outcome per saga")
                        .thresholdSource("workflow:" + workflow.getName() + ".terminal-states")
                        .evidenceEvents(terminalEvidence)
                        .measuredAt(lastTimestamp(terminalEvidence)));
    }

    public void detectDuplicateCompensation(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;

        List<SagaEvent> duplicateStarts = firstDuplicateCompensationStarts(workflow, events);
        if (duplicateStarts.size() < 2) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.DUPLICATE_COMPENSATION)) return;

        String types = distinctTypes(duplicateStarts);
        createAnomaly(sagaId, AnomalyType.DUPLICATE_COMPENSATION,
                String.format("Compensation started %d times (%s) - duplicate compensation",
                        duplicateStarts.size(), types),
                details()
                        .observedValue(String.format("%s start count=%d", types, duplicateStarts.size()))
                        .expectedValue("compensation starts once per compensation attempt")
                        .thresholdSource("workflow:" + workflow.getName() + ".compensation.cardinality")
                        .evidenceEvents(duplicateStarts)
                        .measuredAt(lastTimestamp(duplicateStarts)));
    }

    private static List<SagaEvent> firstDuplicateCompensationStarts(WorkflowSpec workflow, List<SagaEvent> events) {
        Map<String, List<SagaEvent>> startsByStep = new LinkedHashMap<>();
        Set<String> completedSteps = new HashSet<>();
        for (SagaEvent event : events) {
            Optional<WorkflowSpec.EventRole> role = workflow.classifyEvent(event.getEventType())
                    .filter(r -> workflow.isCompensationStep(r.stepId()));
            if (role.isEmpty()) continue;

            String step = role.get().stepId();
            if (role.get().role() == WorkflowSpec.EventRole.Role.COMPLETED) completedSteps.add(step);
            if (role.get().role() != WorkflowSpec.EventRole.Role.STARTED) continue;

            List<SagaEvent> starts = startsByStep.computeIfAbsent(step, k -> new ArrayList<>());
            SagaEvent previousStart = starts.isEmpty() ? null : starts.get(starts.size() - 1);
            starts.add(event);
            if (previousStart == null) continue;

            boolean restartedAfterCompletion = completedSteps.contains(step);
            boolean restartedBeforeTimeout = startedWithinTimeout(workflow, step, previousStart, event);
            if (restartedAfterCompletion || restartedBeforeTimeout) return List.copyOf(starts);
        }
        return List.of();
    }

    private static boolean startedWithinTimeout(WorkflowSpec workflow, String step,
            SagaEvent previousStart, SagaEvent nextStart) {
        if (previousStart.getTimestamp() == null || nextStart.getTimestamp() == null) return false;
        long secondsBetween = Duration.between(previousStart.getTimestamp(), nextStart.getTimestamp()).getSeconds();
        return secondsBetween < workflow.getStepTimeoutSeconds(step);
    }

    public void withdrawUnprovenOrder(String sagaId, AnomalyType type) {
        if (!anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, type)) return;
        int withdrawn = anomalyRepository.retractClaims(sagaId, Set.of(type), Instant.now(),
                "the order it rested on came from clocks; later observations declare a causal "
                        + "order under which it no longer holds");
        if (withdrawn > 0) {
            log.info("Withdrew {} against execution {}: declared causation reorders its pair",
                    type, sagaId);
        }
    }

    public void withdrawMissingTerminal(String sagaId, SagaStatus endedAs) {
        if (!anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MISSING_TERMINAL_EVENT)) return;
        int withdrawn = anomalyRepository.retractClaims(sagaId,
                Set.of(AnomalyType.MISSING_TERMINAL_EVENT), Instant.now(),
                "the run was observed to end as " + endedAs + " after this finding was made");
        if (withdrawn > 0) {
            log.info("Withdrew MISSING_TERMINAL_EVENT against execution {}: it ended as {}",
                    sagaId, endedAs);
        }
    }

    private Set<String> anyWorkflowStart() {
        return workflowRegistry.workflowNames().stream()
                .map(workflowRegistry::byType).flatMap(Optional::stream)
                .flatMap(spec -> spec.startEventTypes().stream())
                .collect(Collectors.toSet());
    }

    public void detectOrphanEvent(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        Set<String> startEventTypes = workflow != null ? workflow.startEventTypes() : anyWorkflowStart();

        boolean startObserved = events.stream()
                .anyMatch(e -> startEventTypes.contains(e.getEventType()));
        if (startObserved) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.ORPHAN_EVENT)) {
                int withdrawn = anomalyRepository.retractClaims(sagaId,
                        Set.of(AnomalyType.ORPHAN_EVENT), Instant.now(),
                        "a start event of " + (workflow != null ? "workflow " + workflow.getName()
                                : "a loaded workflow") + " was observed after this finding was made");
                if (withdrawn > 0) {
                    log.info("Withdrew the orphan finding against execution {}: its start "
                            + "event arrived after the rest of the run", sagaId);
                }
            }
            return;
        }

        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.ORPHAN_EVENT)) return;

        SagaEvent earliest = events.isEmpty() ? null : events.get(0);
        if (earliest == null) return;

        String expected = workflow == null
                ? "an event that some loaded workflow declares as a start"
                : "one of " + startEventTypes + " to start " + workflow.getName();
        createAnomaly(sagaId, AnomalyType.ORPHAN_EVENT,
                String.format("No event that starts %s has been observed for saga %s "
                                + "(earliest seen: '%s') — possible wrong correlation key",
                        workflow != null ? workflow.getName() : "any known workflow",
                        sagaId, earliest.getEventType()),
                details()
                        .observedValue(String.format("earliest observed event is %s",
                                earliest.getEventType()))
                        .expectedValue(expected)
                        .thresholdSource(workflow != null
                                ? "workflow:" + workflow.getName() + ".start_event"
                                : "registry:no-workflow-resolved")
                        .evidenceEvents(earliest)
                        .measuredAt(earliest.getTimestamp()));
    }

    public void detectMissingTraceContext(String sagaId, List<SagaEvent> sagaEvents) {
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MISSING_TRACE_CONTEXT)) return;

        boolean sagaHasAnyTrace = sagaEvents.stream().anyMatch(AnomalyDetectionService::hasTrace);
        if (!sagaHasAnyTrace) return;

        SagaEvent untraced = sagaEvents.stream()
                .filter(e -> !hasTrace(e))
                .findFirst()
                .orElse(null);
        if (untraced == null) return;

        createAnomaly(sagaId, AnomalyType.MISSING_TRACE_CONTEXT,
                String.format("Event '%s' missing traceId - trace propagation broken",
                        untraced.getEventType()),
                details()
                        .observedValue(String.format("event '%s' reports no trace id",
                                untraced.getEventType()))
                        .expectedValue("a trace id at the path this source's mapping declares")
                        .thresholdSource("otel:trace-context-propagation")
                        .evidenceEvents(untraced)
                        .measuredAt(untraced.getTimestamp()));
    }

    public int retractAbsenceClaims(String sagaId, String reason) {
        return anomalyRepository.retractClaims(
                sagaId, AnomalyType.absenceClaims(), Instant.now(), reason);
    }

    private static Instant lastHeardAt(SagaEvent event) {
        return event.getReceivedAt() != null ? event.getReceivedAt() : event.getTimestamp();
    }

    private static boolean hasTrace(SagaEvent event) {
        return event != null && event.getTraceId() != null && !event.getTraceId().isBlank();
    }

    public void detectStuckSagas() {
        List<SagaInstance> runningSagas = sagaInstanceRepository.findByStatusIn(
                RUNNING_STATUSES, detectionBatch());

        Instant now = Instant.now();

        for (SagaInstance saga : runningSagas) {
            if (saga.hasWorkflowConflict()) continue;
            WorkflowSpec workflow = specFor(saga);
            List<SagaEvent> stored = sagaEventRepository.findBySagaIdOrderByTimestampAsc(saga.getSagaId());

            SagaEvent lastEvent = withoutEmptyRepeats(workflow, stored).stream()
                    .max(Comparator.comparing(AnomalyDetectionService::lastHeardAt))
                    .orElse(null);
            if (lastEvent == null) continue;

            EvidenceTimeline timeline = workflow == null ? null : EvidenceTimeline.of(workflow, stored);

            if (timeline != null && timeline.endedAt().isPresent()) continue;
            List<SagaEvent> inOrder = timeline == null ? List.of() : timeline.inBusinessOrder();
            List<String> awaited = awaitedSteps(workflow, inOrder);
            int timeoutSeconds = silenceTimeoutSeconds(workflow, awaited);
            String timeoutSource = awaited.isEmpty()
                    ? "config:anomaly.detection.default-step-timeout-seconds"
                    : "workflow:" + String.join("|", awaited) + ".timeout";

            Instant lastProgressAt = lastHeardAt(lastEvent);
            boolean silenceExceeded = lastProgressAt.plus(timeoutSeconds, ChronoUnit.SECONDS).isBefore(now);
            if (!silenceExceeded || isStuckAlreadyReportedSince(saga.getSagaId(), lastProgressAt)) continue;

            SagaEvent cited = lastPlaced(workflow, inOrder).orElse(lastEvent);
            long ignoredRepeats = countRepeatsWithoutProgressAfter(workflow, stored, lastProgressAt);
            String repeats = ignoredRepeats == 0 ? ""
                    : String.format(", %d repeat(s) without progress ignored", ignoredRepeats);
            createAnomaly(saga.getSagaId(), AnomalyType.SAGA_STUCK,
                    String.format("Saga stuck: no progress since %s%s (last: %s, "
                                    + "stamped %s, timeout: %ds)",
                            lastProgressAt, repeats, cited.getEventType(),
                            cited.getTimestamp(), timeoutSeconds),
                    details()
                            .observedValue(String.format("no progress since %s%s, after %s",
                                    lastProgressAt, repeats, cited.getEventType()))
                            .expectedValue(String.format("next event within %ds", timeoutSeconds))
                            .thresholdSource(timeoutSource)
                            .evidenceEvents(cited)
                            .measuredAt(now));
        }
    }

    private boolean isStuckAlreadyReportedSince(String sagaId, Instant lastProgressAt) {
        return anomalyRepository.findStandingBySagaId(sagaId).stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK
                        && a.getDetectedAt().isAfter(lastProgressAt));
    }

    private static long countRepeatsWithoutProgressAfter(WorkflowSpec workflow, List<SagaEvent> events,
            Instant after) {
        if (workflow == null) return 0;
        return events.stream()
                .filter(e -> workflow.isRepeatWithoutProgress(e.getEventType()))
                .filter(e -> lastHeardAt(e).isAfter(after))
                .count();
    }

    private static List<SagaEvent> withoutEmptyRepeats(WorkflowSpec workflow, List<SagaEvent> events) {
        if (workflow == null) return events;
        return events.stream().filter(e -> !workflow.isRepeatWithoutProgress(e.getEventType())).toList();
    }

    private List<String> awaitedSteps(WorkflowSpec workflow, List<SagaEvent> inOrder) {
        return lastPlaced(workflow, inOrder).map(event -> {
            String type = event.getEventType();
            List<String> next = workflow.stepsAwaitedAfter(type);
            return next.isEmpty() ? List.of(workflow.getExpectedStepForEvent(type)) : next;
        }).orElse(List.of());
    }

    private static Optional<SagaEvent> lastPlaced(WorkflowSpec workflow, List<SagaEvent> inOrder) {
        if (workflow == null) return Optional.empty();
        for (int i = inOrder.size() - 1; i >= 0; i--) {
            if (workflow.getExpectedStepForEvent(inOrder.get(i).getEventType()) != null) {
                return Optional.of(inOrder.get(i));
            }
        }
        return Optional.empty();
    }

    private int silenceTimeoutSeconds(WorkflowSpec workflow, List<String> awaited) {
        return awaited.stream().mapToInt(step -> workflow.getStepTimeoutSeconds(step)).max()
                .orElse(detectionProperties.getDefaultStepTimeoutSeconds());
    }

    public void withdrawDisprovenSilence(WorkflowSpec workflow, String sagaId, List<SagaEvent> observed) {
        if (!anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.SAGA_STUCK)) return;
        List<SagaEvent> inOrder = withoutEmptyRepeats(workflow, observed);
        Instant now = Instant.now();
        for (Anomaly anomaly : anomalyRepository.findStandingBySagaId(sagaId)) {
            if (anomaly.getAnomalyType() != AnomalyType.SAGA_STUCK) continue;
            Instant raisedAt = anomaly.getDetectedAt();
            List<SagaEvent> inHand = inOrder.stream()
                    .filter(e -> lastHeardAt(e).isBefore(raisedAt)).toList();
            SagaEvent lastInHand = inHand.stream().max(Comparator.comparing(SagaEvent::getTimestamp)).orElse(null);
            SagaEvent firstLate = inOrder.stream().filter(e -> !lastHeardAt(e).isBefore(raisedAt))
                    .min(Comparator.comparing(SagaEvent::getTimestamp)).orElse(null);
            if (lastInHand == null || firstLate == null) continue;
            int timeout = silenceTimeoutSeconds(workflow, awaitedSteps(workflow, inHand));
            if (Duration.between(lastInHand.getTimestamp(), firstLate.getTimestamp()).getSeconds() > timeout) continue;
            anomaly.setRetractedAt(now);
            anomaly.setRetractionReason(String.format("%s, read after this finding, happened within %ds "
                    + "of %s by the source's stamps; the silence was the observer reading late",
                    firstLate.getEventType(), timeout, lastInHand.getEventType()));
            anomalyRepository.save(anomaly);
            log.info("Withdrew SAGA_STUCK against execution {}: {} arrived late but happened in time",
                    sagaId, firstLate.getEventType());
        }
    }

    public void withdrawStartedCompensation(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;
        if (!anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.COMPENSATION_MISSING)) return;
        if (compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.STARTED).isEmpty()) return;
        int withdrawn = anomalyRepository.retractClaims(sagaId, Set.of(AnomalyType.COMPENSATION_MISSING),
                Instant.now(), "a compensation start was observed after this finding was made");
        if (withdrawn > 0) {
            log.info("Withdrew COMPENSATION_MISSING against execution {}: compensation started", sagaId);
        }
    }

    public void detectStepTimeouts() {
        Instant recentCutoff = Instant.now().minus(detectionProperties.getCompletedSagaLookback());
        List<SagaInstance> activeSagas = new ArrayList<>(
                sagaInstanceRepository.findByStatusIn(IN_FLIGHT_STATUSES, detectionBatch()));
        activeSagas.addAll(sagaInstanceRepository.findByStatusInAndEndTimeAfter(
                SETTLED_STATUSES, recentCutoff, detectionBatch()));

        Map<String, List<SagaEvent>> eventsBySaga = eventsFor(activeSagas, AnomalyType.STEP_TIMEOUT);

        Map<String, WorkflowSpec> specBySaga = activeSagas.stream()
                .filter(saga -> specFor(saga) != null)
                .collect(Collectors.toMap(SagaInstance::getSagaId, this::specFor, (a, b) -> a));

        for (Map.Entry<String, List<SagaEvent>> entry : eventsBySaga.entrySet()) {
            if (entry.getValue().size() < 2) continue;
            WorkflowSpec workflow = specBySaga.get(entry.getKey());
            if (workflow == null) continue;
            evaluateStepTimeouts(entry.getKey(), entry.getValue(), workflow);
        }
    }

    private Map<String, List<SagaEvent>> eventsFor(List<SagaInstance> sagas, AnomalyType alreadyDetected) {
        if (sagas.isEmpty()) return Map.of();

        List<String> sagaIds = sagas.stream().map(SagaInstance::getSagaId).distinct().toList();
        Set<String> skip = Set.copyOf(
                anomalyRepository.findSagaIdsWithAnomalyType(alreadyDetected, sagaIds));

        List<String> pending = sagaIds.stream().filter(id -> !skip.contains(id)).toList();
        if (pending.isEmpty()) return Map.of();

        return sagaEventRepository.findBySagaIdInOrderBySagaIdAscTimestampAsc(pending).stream()
                .collect(Collectors.groupingBy(SagaEvent::getSagaId,
                        LinkedHashMap::new, Collectors.toList()));
    }

    void evaluateStepTimeouts(String sagaId, List<SagaEvent> events, WorkflowSpec workflow) {
        DetectionProperties.StepTimeoutMode mode = detectionProperties.getStepTimeoutMode();
        boolean shadow = mode == DetectionProperties.StepTimeoutMode.SHADOW;
        boolean useProjection = mode == DetectionProperties.StepTimeoutMode.PROJECTION;
        boolean useLegacy = mode == DetectionProperties.StepTimeoutMode.LEGACY;

        StepTimeoutFinding legacy = useLegacy || shadow
                ? findStepTimeoutByAdjacency(events, workflow) : null;
        StepTimeoutFinding projected = useProjection || shadow
                ? findStepTimeoutByProjection(sagaId, events, workflow) : null;

        if (shadow) logDivergence(sagaId, legacy, projected);

        StepTimeoutFinding authoritative = useProjection ? projected : legacy;
        if (authoritative != null) {
            createAnomaly(sagaId, AnomalyType.STEP_TIMEOUT, authoritative.description(),
                    authoritative.details());
        }
    }

    private StepTimeoutFinding findStepTimeoutByAdjacency(List<SagaEvent> events, WorkflowSpec workflow) {
        for (int i = 1; i < events.size(); i++) {
            SagaEvent previousEvent = events.get(i - 1);
            SagaEvent currentEvent = events.get(i);
            long stepDurationMs = Duration.between(
                    previousEvent.getTimestamp(),
                    currentEvent.getTimestamp()).toMillis();

            String stepName = workflow.getExpectedStepForEvent(currentEvent.getEventType());
            if (stepName == null) continue;
            int expectedTimeoutMs = workflow.getStepTimeoutSeconds(stepName) * 1000;

            if (stepDurationMs > expectedTimeoutMs) {
                return new StepTimeoutFinding(stepName, stepDurationMs, expectedTimeoutMs,
                        String.format("Step '%s' took %dms (timeout: %dms). Previous event: %s%s",
                                stepName, stepDurationMs, expectedTimeoutMs,
                                previousEvent.getEventType(), clockCaveat(previousEvent, currentEvent)),
                        details()
                                .observedValue(String.format("%s took %dms; %s",
                                        stepName, stepDurationMs, clocks(previousEvent, currentEvent)))
                                .expectedValue(String.format("<= %dms", expectedTimeoutMs))
                                .thresholdSource("workflow:" + stepName + ".timeout")
                                .evidenceEvents(previousEvent, currentEvent)
                                .measuredAt(currentEvent.getTimestamp()));
            }
        }
        return null;
    }

    private StepTimeoutFinding findStepTimeoutByProjection(String sagaId, List<SagaEvent> events,
            WorkflowSpec workflow) {
        ExecutionProjection projection = executionProjectionService.project(sagaId, events, workflow);

        for (ExecutionProjection.Operation op : projection.measurableOperations()) {
            long durationMs = op.duration().toMillis();
            int expectedTimeoutMs = workflow.getStepTimeoutSeconds(op.stepId()) * 1000;
            if (durationMs <= expectedTimeoutMs) continue;

            String provenance = op.startProvenance().name();
            SagaEvent start = byId(events, op.startEventId());
            SagaEvent end = byId(events, op.endEventId());
            return new StepTimeoutFinding(op.stepId(), durationMs, expectedTimeoutMs,
                    String.format("Operation '%s' took %dms across %d attempt(s) (timeout: %dms), "
                                    + "measured from %s%s",
                            op.stepId(), durationMs, op.attemptCount(), expectedTimeoutMs, provenance,
                            clockCaveat(start, end)),
                    details()
                            .observedValue(String.format("%s took %dms over %d attempt(s); start=%s; %s",
                                    op.stepId(), durationMs, op.attemptCount(), provenance,
                                    clocks(start, end)))
                            .expectedValue(String.format("<= %dms", expectedTimeoutMs))
                            .thresholdSource("workflow:" + op.stepId() + ".timeout")
                            .evidenceEventIds(op.evidenceEventIds().toArray(new String[0]))
                            .measuredAt(op.endedAt()));
        }
        return null;
    }

    private void logDivergence(String sagaId, StepTimeoutFinding legacy, StepTimeoutFinding projected) {
        boolean legacyFired = legacy != null;
        boolean projectedFired = projected != null;

        if (!legacyFired && !projectedFired) return;

        if (legacyFired != projectedFired) {
            log.warn("STEP_TIMEOUT shadow divergence saga={}: legacy={} projection={}",
                    sagaId,
                    legacyFired ? legacy.summary() : "no-finding",
                    projectedFired ? projected.summary() : "no-finding");
        } else if (!legacy.stepId().equals(projected.stepId())
                || legacy.durationMs() != projected.durationMs()) {
            log.warn("STEP_TIMEOUT shadow divergence saga={}: legacy={} projection={}",
                    sagaId, legacy.summary(), projected.summary());
        } else {
            log.debug("STEP_TIMEOUT shadow agreement saga={}: {}", sagaId, legacy.summary());
        }
    }

    static String clocks(SagaEvent from, SagaEvent to) {
        return clocks(from != null ? from.getEventTimeSource() : null,
                to != null ? to.getEventTimeSource() : null);
    }

    static String clocks(EventTimeSource a, EventTimeSource b) {
        if (a == null || b == null) return "clocks=unrecorded";
        return "clocks=" + a + "->" + b + (a == b ? "" : " (mixed)");
    }

    private static String clockCaveat(SagaEvent from, SagaEvent to) {
        EventTimeSource a = from != null ? from.getEventTimeSource() : null;
        EventTimeSource b = to != null ? to.getEventTimeSource() : null;
        if (a == null || b == null) {
            return ". Where the two timestamps came from was not recorded";
        }
        return a == b ? "" : String.format(". The two ends were taken from different clocks (%s, %s), "
                + "so the interval includes whatever separates them", a, b);
    }

    private static SagaEvent byId(List<SagaEvent> events, String eventId) {
        if (eventId == null) return null;
        return events.stream().filter(e -> eventId.equals(e.getEventId())).findFirst().orElse(null);
    }

    private record StepTimeoutFinding(String stepId, long durationMs, int thresholdMs,
            String description, AnomalyDetails details) {
        String summary() {
            return String.format("%s=%dms(>%dms)", stepId, durationMs, thresholdMs);
        }
    }

    public void detectMissingTerminalEvents() {
        int multiplier = detectionProperties.getMissingTerminalTimeoutMultiplier();
        Instant now = Instant.now();

        Instant cutoff = now.minus((long) minStepTimeoutAcrossWorkflows() * multiplier, ChronoUnit.SECONDS);

        List<SagaInstance> stuckSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedBefore(
                List.of(SagaStatus.STARTED, SagaStatus.IN_PROGRESS), cutoff, detectionBatch());

        for (SagaInstance saga : stuckSagas) {
            if (saga.hasWorkflowConflict()) continue;
            String sagaId = saga.getSagaId();
            WorkflowSpec spec = specFor(saga);
            int maxTimeout = maxStepTimeoutOf(spec) * multiplier;
            if (saga.getLastUpdated() != null
                    && saga.getLastUpdated().isAfter(now.minus(maxTimeout, ChronoUnit.SECONDS))) continue;
            if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MISSING_TERMINAL_EVENT)) continue;
            if (spec != null && hasEnded(spec, sagaId)) continue;

            long eventCount = sagaEventRepository.countBySagaId(sagaId);
            if (eventCount < 2) continue;

            SagaEvent lastEvent = sagaEventRepository.findTopBySagaIdOrderByTimestampDesc(sagaId).orElse(null);
            Instant detectedAt = Instant.now();
            createAnomaly(sagaId, AnomalyType.MISSING_TERMINAL_EVENT,
                    String.format("Saga had %d events at %s (when detected) but no terminal event after %ds",
                            eventCount, detectedAt, maxTimeout),
                    details()
                            .observedValue(String.format("%d events at detection (%s) and no terminal event",
                                    eventCount, detectedAt))
                            .expectedValue(String.format("terminal event within %ds", maxTimeout))
                            .thresholdSource("config:anomaly.detection.missing-terminal-timeout-multiplier")
                            .evidenceEvents(lastEvent)
                            .measuredAt(Instant.now()));
        }
    }

    private boolean hasEnded(WorkflowSpec spec, String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        return EvidenceTimeline.of(spec, events).endedAt().isPresent();
    }

    public void detectCompensationMissing() {
        Instant cutoff = Instant.now().minus(detectionProperties.getCompensationMissingCutoff());

        List<SagaInstance> compensatingSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedBefore(
                List.of(SagaStatus.COMPENSATING), cutoff, detectionBatch());

        for (SagaInstance saga : compensatingSagas) {
            if (saga.getLastUpdated().isAfter(cutoff)) continue;
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.COMPENSATION_MISSING)) continue;

            WorkflowSpec workflow = specFor(saga);
            if (workflow == null) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            List<SagaEvent> failureEvents = eventsWithRole(workflow, events, WorkflowSpec.EventRole.Role.FAILED);
            boolean hasFailure = !failureEvents.isEmpty();
            boolean hasCompStart = !compensationEvents(
                    workflow, events, WorkflowSpec.EventRole.Role.STARTED).isEmpty();

            if (hasFailure && !hasCompStart) {
                String failureTypes = distinctTypes(failureEvents);
                createAnomaly(saga.getSagaId(), AnomalyType.COMPENSATION_MISSING,
                        String.format("Failure detected (%s) but no compensation started", failureTypes),
                        details()
                                .observedValue(failureTypes + " without any compensation start event")
                                .expectedValue("a compensation step starts after a step failure")
                                .thresholdSource("workflow:" + workflow.getName() + ".compensation")
                                .evidenceEvents(failureEvents)
                                .measuredAt(Instant.now()));
            }
        }
    }

    public void detectMissingCompensationAck() {
        Instant cutoff = Instant.now().minus(detectionProperties.getCompensationAckCutoff());

        List<SagaInstance> compensatingSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedBefore(
                List.of(SagaStatus.COMPENSATING), cutoff, detectionBatch());

        for (SagaInstance saga : compensatingSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK)) continue;

            WorkflowSpec workflow = specFor(saga);
            if (workflow == null) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            List<SagaEvent> owed = owedAcknowledgements(workflow, events);

            if (!owed.isEmpty()) {
                String startedTypes = distinctTypes(owed);
                createAnomaly(saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK,
                        String.format("Compensation started (%s) but never acknowledged", startedTypes),
                        details()
                                .observedValue(startedTypes + " without a compensation completion event")
                                .expectedValue("the compensation step reports completion, which this "
                                        + "workflow declares it does")
                                .thresholdSource("workflow:" + workflow.getName() + ".compensation.ack")
                                .evidenceEvents(owed)
                                .measuredAt(Instant.now()));
            }
        }
    }

    private List<SagaEvent> owedAcknowledgements(WorkflowSpec workflow, List<SagaEvent> events) {
        Set<String> acknowledgedSteps = stepIdsOf(workflow, compensationEvents(
                workflow, events, WorkflowSpec.EventRole.Role.COMPLETED));
        return compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.STARTED).stream()
                .filter(event -> workflow.classifyEvent(event.getEventType())
                        .map(role -> workflow.declaresCompletionEvent(role.stepId())
                                && !acknowledgedSteps.contains(role.stepId()))
                        .orElse(false))
                .toList();
    }

    public void withdrawAcknowledgedCompensation(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;
        if (!anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MISSING_COMPENSATION_ACK)) return;
        if (!owedAcknowledgements(workflow, events).isEmpty()) return;
        int withdrawn = anomalyRepository.retractClaims(sagaId,
                Set.of(AnomalyType.MISSING_COMPENSATION_ACK), Instant.now(),
                "the compensation acknowledgement was observed after this finding was made");
        if (withdrawn > 0) {
            log.info("Withdrew MISSING_COMPENSATION_ACK against execution {}: acknowledged late", sagaId);
        }
    }

    private Set<String> stepIdsOf(WorkflowSpec workflow, List<SagaEvent> events) {
        return events.stream()
                .map(event -> workflow.classifyEvent(event.getEventType()).orElse(null))
                .filter(Objects::nonNull)
                .map(WorkflowSpec.EventRole::stepId)
                .collect(Collectors.toSet());
    }

    public void detectDelayedCompensation() {
        Instant recentCutoff = Instant.now().minus(detectionProperties.getCompensationRecentLookback());

        List<SagaInstance> compensatedSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedAfter(
                List.of(SagaStatus.COMPENSATED, SagaStatus.COMPENSATING), recentCutoff, detectionBatch());

        for (SagaInstance saga : compensatedSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.DELAYED_COMPENSATION)) continue;

            WorkflowSpec workflow = specFor(saga);
            if (workflow == null) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());

            SagaEvent failureEvent = eventsWithRole(
                    workflow, events, WorkflowSpec.EventRole.Role.FAILED).stream()
                    .findFirst().orElse(null);
            SagaEvent compensationStartEvent = compensationEvents(
                    workflow, events, WorkflowSpec.EventRole.Role.STARTED).stream()
                    .findFirst().orElse(null);

            if (failureEvent == null || compensationStartEvent == null) continue;

            int compensationTimeoutSeconds = workflow
                    .classifyEvent(compensationStartEvent.getEventType())
                    .map(role -> workflow.getStepTimeoutSeconds(role.stepId()))
                    .orElse(detectionProperties.getDefaultStepTimeoutSeconds());
            int compensationThresholdMs = (int) (compensationTimeoutSeconds
                    * detectionProperties.getDelayedCompensationMultiplier() * 1000);

            long gapMs = Duration.between(failureEvent.getTimestamp(),
                    compensationStartEvent.getTimestamp()).toMillis();
            if (gapMs > compensationThresholdMs) {
                createAnomaly(saga.getSagaId(), AnomalyType.DELAYED_COMPENSATION,
                        String.format("Compensation delayed: %dms gap between %s and %s (threshold: %dms)",
                                gapMs, failureEvent.getEventType(),
                                compensationStartEvent.getEventType(), compensationThresholdMs),
                        details()
                                .observedValue(String.format("%dms gap from %s to %s",
                                        gapMs, failureEvent.getEventType(),
                                        compensationStartEvent.getEventType()))
                                .expectedValue(String.format("<= %dms", compensationThresholdMs))
                                .thresholdSource("config:anomaly.detection.delayed-compensation-multiplier")
                                .evidenceEvents(failureEvent, compensationStartEvent)
                                .measuredAt(compensationStartEvent.getTimestamp()));
            }
        }
    }

    private WorkflowSpec specFor(SagaInstance saga) {
        return saga.hasWorkflowConflict() ? null : workflowRegistry.byType(saga.getWorkflowType()).orElse(null);
    }

    private List<SagaEvent> eventsWithRole(WorkflowSpec workflow, List<SagaEvent> events,
            WorkflowSpec.EventRole.Role role) {
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(r -> r.role() == role && !workflow.isCompensationStep(r.stepId()))
                        .isPresent())
                .toList();
    }

    private List<SagaEvent> compensationEvents(WorkflowSpec workflow, List<SagaEvent> events,
            WorkflowSpec.EventRole.Role role) {
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(r -> r.role() == role && workflow.isCompensationStep(r.stepId()))
                        .isPresent())
                .toList();
    }

    private static String distinctTypes(List<SagaEvent> events) {
        return events.stream()
                .map(SagaEvent::getEventType)
                .distinct()
                .collect(Collectors.joining(", "));
    }

    private int maxStepTimeoutOf(WorkflowSpec spec) {
        if (spec == null || spec.getSteps() == null) return detectionProperties.getDefaultStepTimeoutSeconds();
        return spec.getSteps().stream()
                .mapToInt(step -> spec.getStepTimeoutSeconds(step.getId()))
                .max()
                .orElse(detectionProperties.getDefaultStepTimeoutSeconds());
    }

    private int minStepTimeoutAcrossWorkflows() {
        return Math.min(detectionProperties.getDefaultStepTimeoutSeconds(),
                workflowRegistry.workflowNames().stream()
                        .map(name -> workflowRegistry.byType(name).orElse(null))
                        .filter(Objects::nonNull)
                        .mapToInt(this::maxStepTimeoutOf)
                        .min()
                        .orElse(detectionProperties.getDefaultStepTimeoutSeconds()));
    }

    private Pageable detectionBatch() {
        return PageRequest.of(0, detectionProperties.getDetectionBatchSize(),
                Sort.by(Sort.Direction.DESC, "lastUpdated"));
    }

    private void createAnomaly(String sagaId, AnomalyType type, String description, AnomalyDetails details) {
        Anomaly anomaly = new Anomaly();
        anomaly.setSagaId(sagaId);
        anomaly.setAnomalyType(type);
        anomaly.setDescription(description);
        anomaly.setDetectedAt(Instant.now());
        anomaly.setObservedValue(details.observedValue);
        anomaly.setExpectedValue(details.expectedValue);
        anomaly.setThresholdSource(details.thresholdSource);
        anomaly.setEvidenceEventIds(details.evidenceEventIds);
        anomaly.setMeasuredAt(details.measuredAt);
        anomalyRepository.save(anomaly);
        log.warn("Anomaly detected: [{}] {} - {}", type, sagaId, description);
    }

    private static AnomalyDetails details() {
        return new AnomalyDetails();
    }

    private static Instant lastTimestamp(List<SagaEvent> events) {
        if (events == null || events.isEmpty()) return null;
        return events.get(events.size() - 1).getTimestamp();
    }

    private static class AnomalyDetails {
        private String observedValue;
        private String expectedValue;
        private String thresholdSource;
        private List<String> evidenceEventIds;
        private Instant measuredAt;

        private AnomalyDetails observedValue(String observedValue) {
            this.observedValue = observedValue;
            return this;
        }

        private AnomalyDetails expectedValue(String expectedValue) {
            this.expectedValue = expectedValue;
            return this;
        }

        private AnomalyDetails thresholdSource(String thresholdSource) {
            this.thresholdSource = thresholdSource;
            return this;
        }

        private AnomalyDetails evidenceEventIds(String... eventIds) {
            this.evidenceEventIds = Arrays.stream(eventIds == null ? new String[0] : eventIds)
                    .filter(Objects::nonNull)
                    .filter(id -> !id.isBlank())
                    .toList();
            return this;
        }

        private AnomalyDetails evidenceEvents(SagaEvent... events) {
            return evidenceEvents(Arrays.asList(events == null ? new SagaEvent[0] : events));
        }

        private AnomalyDetails evidenceEvents(List<SagaEvent> events) {
            if (events == null) {
                this.evidenceEventIds = List.of();
                return this;
            }
            this.evidenceEventIds = events.stream()
                    .filter(Objects::nonNull)
                    .map(SagaEvent::getEventId)
                    .filter(Objects::nonNull)
                    .filter(id -> !id.isBlank())
                    .toList();
            return this;
        }

        private AnomalyDetails measuredAt(Instant measuredAt) {
            this.measuredAt = measuredAt;
            return this;
        }
    }
}
