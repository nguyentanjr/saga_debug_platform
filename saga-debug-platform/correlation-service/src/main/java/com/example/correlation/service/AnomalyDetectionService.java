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


    /**
     * @param sagaEvents every event of the saga, including the one just stored.
     *                   Passed in rather than re-queried: the ingestion path used to
     *                   reload the whole event list once per detector, so a saga with
     *                   n events cost O(n^2) row reads over its lifetime.
     */
    public void detectOutOfOrder(String sagaId, List<SagaEvent> sagaEvents) {
        List<SagaEvent> events = sagaEvents.stream()
                .sorted(Comparator.comparing(SagaEvent::getReceivedAt,
                        Comparator.nullsLast(Comparator.naturalOrder())))
                .toList();
        if (events.size() < 2) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.OUT_OF_ORDER)) return;

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
                return;
            }
        }
    }



    /**
     * A run that went somewhere its workflow does not allow.
     *
     * <p>Only a declared-and-contradicted transition is an anomaly. UNKNOWN covers two
     * quite different silences and neither is a fault in the execution: the specification
     * having nothing to say about this step, and the platform not having observed the steps
     * that lie between these two. The second used to be reported as INVALID, which made an
     * ordinary gap in delivery — a partition read late, a retry still in flight — into a
     * standing accusation against the observed system. It is logged rather than filed,
     * because an operator looking at a quiet run is entitled to know which observations the
     * platform was waiting on.
     */
    /** @return whether the workflow forbids this handoff */
    public boolean detectInvalidTransition(WorkflowSpec workflow, String sagaId, String previousStep,
            String currentStep, String condition, String eventId, Instant eventTimestamp) {
        if (workflow == null || workflow.getTransitions() == null) return false;

        if (previousStep == null) return false;
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

        {
            // Every arrival re-judges every adjacent pair, so the same handoff reaches here
            // again with each later observation. It is one occurrence and stands once (F14).
            String observed = String.format("%s -> %s on %s", previousStep, currentStep, condition);
            boolean alreadyStanding = anomalyRepository.findStandingBySagaId(sagaId).stream()
                    .anyMatch(a -> a.getAnomalyType() == AnomalyType.INVALID_TRANSITION
                            && observed.equals(a.getObservedValue())
                            && Objects.equals(a.getEvidenceEventIds(), details().evidenceEventIds(eventId).evidenceEventIds));
            if (alreadyStanding) return true;

            String expected = workflow.getTransitions().stream()
                    .filter(t -> t.getFrom().equals(previousStep))
                    .map(t -> t.getTo() + " (on: " + t.getCondition() + ")")
                    .collect(Collectors.joining(", "));

            createAnomaly(sagaId, AnomalyType.INVALID_TRANSITION,
                    String.format("Invalid transition: %s -> %s (condition: %s). Expected from %s: [%s]",
                            previousStep, currentStep, condition, previousStep,
                            expected.isEmpty() ? "none defined" : expected),
                    details()
                            .observedValue(String.format("%s -> %s on %s", previousStep, currentStep, condition))
                            .expectedValue(expected.isEmpty() ? "no transition defined from " + previousStep : expected)
                            .thresholdSource("workflow:" + workflow.getName() + ".transitions")
                            .evidenceEventIds(eventId)
                            .measuredAt(eventTimestamp));
        }
        return true;
    }

    /**
     * EXT-I9: withdraws the invalid transitions the whole evidence no longer shows.
     *
     * <p>A handoff is judged between neighbours in business order, and on a partial
     * timeline two observations can be neighbours that the full one separates -- reverse
     * delivery of a KMS both-failed run left INVALID_TRANSITION standing that forward
     * delivery never raised. The occurrence was never established; the pair it rested on
     * was an artefact of what had arrived. Findings for handoffs the final timeline still
     * judges invalid are kept.
     *
     * @param stillInvalid {@link #transitionKey} of every handoff judged invalid on the
     *                     current evidence
     */
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

    /** Identifies one judged handoff: what was observed, and the event that made it. */
    public static String transitionKey(String observedHandoff, String eventId) {
        return observedHandoff + "|" + eventId;
    }

    /** The observed value an invalid transition is recorded under. */
    public static String handoff(String previousStep, String currentStep, String condition) {
        return String.format("%s -> %s on %s", previousStep, currentStep, condition);
    }

    public void detectRetryBurst(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        int burstThreshold = detectionProperties.getRetryBurstThreshold();
        Duration burstWindow = detectionProperties.getRetryBurstWindow();
        if (events.size() < burstThreshold) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.RETRY_BURST)) return;

        // No PAYMENT_RETRY fallback: a workflow that declares no retry event does not
        // have retries as far as this platform can tell, and borrowing the order saga's
        // retry event name would attribute another workflow's events to a step it has no
        // relationship with.
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

    /**
     * States how the observed retry count compares with the step's declared attempt
     * budget.
     *
     * <p>The engine-side burst threshold and the service's Resilience4j
     * {@code maxAttempts} were chosen to match each other, so a firing RETRY_BURST does
     * not by itself show that anything exceeded its policy. Recording both numbers in
     * the evidence makes that visible to whoever reads the finding, instead of leaving
     * "retry storm" to imply a violation that was never measured.
     */
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

    /**
     * One event id, delivered by the source more than once.
     *
     * <p>The caller has already established that this is the observed system's doing and
     * not this platform's: a re-read of one record never reaches here. The two transport
     * references are carried into the evidence because they are what settles that
     * question, and a reader who doubts the finding should be able to check it rather
     * than take it on trust.
     *
     * @param storedOn  the record the stored copy arrived on, or null when unrecorded
     * @param arrivedOn the record this copy arrived on, or null when the transport offers
     *                  no identity — in which case the two cannot be compared and this
     *                  keeps its previous meaning: the same event id was delivered again
     */
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

    /**
     * A run that moved backwards through its own lifecycle.
     *
     * <p>Takes the pair the timeline found rather than a before-and-after pair of stored
     * statuses. Those two things used to be conflated, and the difference is the whole
     * finding: the stored status changes whenever a <em>read</em> lands on an event that
     * implies something else, so an execution whose completion was read before its middle
     * step produced COMPLETED → IN_PROGRESS and was reported for it. Nothing had regressed
     * — the platform had read the run backwards and blamed the run.
     *
     * <p>{@link EvidenceTimeline} only ever offers a pair it can order from the evidence,
     * and whose earlier half is terminal, so what arrives here is an observation that
     * really did happen after the run had ended and contradicted that ending. The terminal
     * requirement is what makes the finding safe to file as an occurrence claim on the
     * partial evidence an observer always has: more observations cannot withdraw it, so it
     * never has to be. See {@code EvidenceTimeline#firstRegression}.
     */
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

    /**
     * An event that happened after the run had already ended.
     *
     * <p>Both events are required, and the caller establishes from the timeline that the
     * terminal one provably precedes the late one. The previous version asked the stored
     * status instead — "had the platform concluded this run was over by the time it got
     * round to this event" — which is true of every event read after a completion,
     * including the ones that happened before it. On a partition-shuffled stream that
     * turned ordinary delivery reordering into a finding against the observed system.
     *
     * <p>The timeline selects the same pair for the same event set. A later delivery can
     * reveal an earlier qualifying pair, so revise the standing finding's citation rather
     * than freezing whichever prefix first proved the occurrence. This does not retract
     * the occurrence or create another one. Only a changed citation advances detectedAt:
     * it records detection of the revised evidence and lets the existing RCA sweep notice
     * that its previous analysis is stale. measuredAt remains the cited business time.
     */
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

    /**
     * One standing occurrence of {@code type} per run, citing what the whole evidence in hand
     * selects rather than what the first prefix to prove the occurrence happened to contain.
     *
     * <p>F9 introduced this for FORBIDDEN_POST_TERMINAL; F11 found STATE_REGRESSION and
     * MULTI_TERMINAL_CONFLICT still returning as soon as a finding existed, so their citation
     * depended on delivery order. A changed citation advances detectedAt so the RCA sweep
     * notices; an unchanged one writes nothing. A late alias merge can leave standing rows
     * from both fragments, and each gets the canonical citation — none is deleted.
     */
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

    /**
     * Two observations that could each be the run's last, with nothing to order them.
     *
     * <p>Raised instead of picking one. The pair carries the same instant and belongs to
     * steps the workflow graph does not connect, so neither the clock nor the
     * specification says which happened first — and they imply different outcomes, so the
     * choice is the run's outcome. An observer that resolves this quietly publishes one of
     * two mutually exclusive endings as fact.
     *
     * <p>An absence claim: it is about what the evidence lacks, so it is withdrawn like
     * any other when more observations arrive.
     */
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

    /**
     * Two contradictory terminal outcomes observed for one saga.
     *
     * <p>Was two hard-coded event sets naming the order saga's success and failure
     * events. The question is now asked of the workflow: which observations imply a
     * terminal status, and did this saga produce both a success and a non-success one.
     */
    public void detectMultiTerminalConflict(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;

        List<SagaEvent> succeeded = terminalEvents(workflow, events, SagaStatus.COMPLETED);
        List<SagaEvent> notSucceeded = new ArrayList<>(
                terminalEvents(workflow, events, SagaStatus.FAILED));
        notSucceeded.addAll(terminalEvents(workflow, events, SagaStatus.COMPENSATED));

        if (succeeded.isEmpty() || notSucceeded.isEmpty()) return;

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

    /** Events whose lifecycle role puts the saga into the given terminal status. */
    private List<SagaEvent> terminalEvents(WorkflowSpec workflow, List<SagaEvent> events,
            SagaStatus terminal) {
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .flatMap(workflow::statusAfter)
                        .filter(status -> status == terminal)
                        .isPresent())
                .toList();
    }

    public void detectDuplicateCompensation(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;
        List<SagaEvent> compStartEvents = compensationEvents(
                workflow, events, WorkflowSpec.EventRole.Role.STARTED);
        long startCount = compStartEvents.size();
        if (startCount > 1 && !anomalyRepository.existsBySagaIdAndAnomalyType(
                sagaId, AnomalyType.DUPLICATE_COMPENSATION)) {
            String types = distinctTypes(compStartEvents);
            createAnomaly(sagaId, AnomalyType.DUPLICATE_COMPENSATION,
                    String.format("Compensation started %d times (%s) - duplicate compensation",
                            startCount, types),
                    details()
                            .observedValue(String.format("%s start count=%d", types, startCount))
                            .expectedValue("compensation starts once per compensation attempt")
                            .thresholdSource("workflow:" + workflow.getName() + ".compensation.cardinality")
                            .evidenceEvents(compStartEvents)
                            .measuredAt(lastTimestamp(compStartEvents)));
        }
    }

    /**
     * "No event that starts this run's workflow has been observed."
     *
     * <h2>Two things this used to get wrong</h2>
     * It was asked once, when the execution row was created, about the single event that
     * created it. So it was really answering "was the first event I happened to read a
     * start event" — a question about the observer's read order. And it was withdrawn only
     * as a side effect of the workflow being resolved late, which coupled a claim about one
     * kind of evidence to a change in another: where the workflow was already known — a
     * source mapping that declares it — the start event could arrive, be stored, and the
     * finding would go on saying it had not.
     *
     * <p>Now it is asked of the whole run, every time, and answered from the evidence: if
     * an observed event starts this workflow the claim is false and is withdrawn; if none
     * does it is true and is raised once.
     *
     * <h2>And it says which workflow</h2>
     * The text named {@code ORDER_CREATED} and cited {@code workflow:ORDER_SAGA.start_event}
     * whatever workflow the run belonged to, so a fulfillment saga was told it should have
     * begun with an event no fulfillment saga emits. That is an assumption of this
     * platform's, presented as the specification's requirement.
     *
     * @param events every observation of this run, the newest included
     */
    /**
     * Withdraws MISSING_TERMINAL_EVENT once the run is observed to end.
     *
     * <p>The finding says no terminal event was seen; the ending falsifies it, and leaving
     * it standing made a recovered run read as never having finished (SC16 lost R06, whose
     * guard is the absence of this finding). SAGA_STUCK is not withdrawn here: a silence
     * that happened at the source stays, and R06 is built on it -- only one the source's
     * stamps disprove is ({@link #withdrawDisprovenSilence}).
     */
    /**
     * Withdraws an ordering finding the whole evidence no longer establishes (F28).
     *
     * <p>FORBIDDEN_POST_TERMINAL and STATE_REGRESSION are occurrences, and an occurrence
     * proven by a declared causal link stays proven: more events only add links. One proven
     * only by clocks does not. When the event that sits causally between the pair arrives,
     * the timeline may place the "late" observation before the ending after all, and the
     * finding then describes the producers' clocks, not the run. It is withdrawn with that
     * reason, as an absence claim is withdrawn when its evidence turns up.
     */
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

    /** Every event some loaded workflow declares as its start. */
    private Set<String> anyWorkflowStart() {
        return workflowRegistry.workflowNames().stream()
                .map(workflowRegistry::byType).flatMap(Optional::stream)
                .flatMap(spec -> spec.startEventTypes().stream())
                .collect(Collectors.toSet());
    }

    public void detectOrphanEvent(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        // EXT-RESOLVE: with no workflow resolved -- typically because two declarations both
        // claim the start event -- the question is still whether any loaded workflow's start
        // is in hand, as the finding's own wording says. Asking it of an empty set reported
        // every unresolved run as orphaned, and R15 read that as a wrong correlation key on
        // runs whose start record and key were both present.
        Set<String> startEventTypes = workflow != null ? workflow.startEventTypes() : anyWorkflowStart();

        boolean startObserved = events.stream()
                .anyMatch(e -> startEventTypes.contains(e.getEventType()));
        if (startObserved) {
            // Cheap: only asked when a start event is actually in hand, which happens once.
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

    


    /**
     * Reports an event that dropped the trace its siblings carry.
     *
     * <p>Reads the trace id from the column the source's mapping filled, not from a
     * payload field named {@code traceId}. That name was this platform's own: a system
     * keeping its trace id under {@code context.trace}, or in a {@code traceparent}
     * header, had every event look untraced, and this detector went permanently silent
     * for it while the RCA report lost its link to the traces.
     *
     * <p>Silence is still the right answer when <em>no</em> event of the saga reports a
     * trace: that source does not publish trace context, which is a fact about the
     * source and not a broken propagation. The anomaly is only for the case that is
     * genuinely wrong — some events carry the trace and this one lost it.
     */
    public void detectMissingTraceContext(String sagaId, List<SagaEvent> sagaEvents) {
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MISSING_TRACE_CONTEXT)) return;

        // Asked of the whole run rather than of the observation that has just arrived.
        // It used to be handed only that one, and asked whether any sibling *already
        // stored* carried a trace -- so reading the untraced event before its traced
        // siblings meant no sibling had one yet, nothing was reported, and it was never
        // looked at again. Whether propagation broke is a property of the run; which event
        // arrived first is a property of the delivery.
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

    /**
     * Withdraws the findings about this execution that later evidence may have falsified.
     *
     * <p>A claim of absence — "no start event was seen", "this saga is stuck", "the
     * compensation was never acknowledged" — is only ever true of the observations in hand.
     * When the picture changes underneath it, leaving it standing means reporting something
     * the platform now knows to be wrong, and deleting it means quietly removing a finding
     * that was already shown to someone. Withdrawing keeps the row and the reason, and the
     * scheduled passes raise it again if it still holds.
     *
     * @return how many findings were withdrawn
     */
    public int retractAbsenceClaims(String sagaId, String reason) {
        return anomalyRepository.retractClaims(
                sagaId, AnomalyType.absenceClaims(), Instant.now(), reason);
    }

    /**
     * The last moment this run can be said to have shown a sign of life.
     *
     * <p>"Stuck" is a claim that nothing has happened for a while, and the elapsed time was
     * measured as {@code now - event.timestamp}: the observer's clock minus the source's.
     * Any lag between the two became elapsed silence, so a backfill, a replay, or simply a
     * source whose clock runs behind made every run it touched look stuck the instant it
     * was ingested. Reproduced live: an ORDER_CREATED stamped ten minutes earlier and
     * injected raised SAGA_STUCK within one scheduler pass, against a run whose event the
     * platform had read a second before.
     *
     * <p>Whether the platform has heard anything is a question only the platform's own
     * clock can answer, and {@code receivedAt} is where that answer is recorded. The
     * source's stamp is not consulted at all here — not even as a tie-break. It says when
     * the work happened, which is the right clock for durations between events and the
     * wrong one for elapsed silence: a source stamping the past invents silence that never
     * occurred, and one stamping the future conceals silence that did.
     *
     * <p>The stamp is still reported in the finding, so an operator can see both.
     */
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
            // EXT-OVERLOAD-FALSE-FINDINGS: the last thing heard is the latest arrival, not the
            // latest source stamp. Under backlog the run's newest-stamped event can have been
            // read minutes before an earlier-stamped one that arrived just now (bench 2000 ev/s:
            // SAGA_STUCK 70 ms after ORDER_CREATED was read, "silent since" PAYMENT_REFUNDED).
            SagaEvent lastEvent = stored.stream()
                    .max(Comparator.comparing(AnomalyDetectionService::lastHeardAt))
                    .orElse(null);
            if (lastEvent == null) continue;

            // EXT-STUCK-TIMEOUT: silence is measured against the step the run is waiting on.
            // That used to be the step of whatever event came last -- the step that had just
            // finished, or, for an event the workflow does not know (ewallet COMMISSION_REQUESTED),
            // the 60s default -- so every W2N transfer waiting its declared 15m for release was
            // reported stuck after a minute. The clock still starts at the last thing heard.
            EvidenceTimeline timeline = workflow == null ? null : EvidenceTimeline.of(workflow, stored);
            // EXT-OVERLOAD-FALSE-FINDINGS: the run list was read before this pass reached the
            // run; an ingest in between can have ended it (bench 2000 ev/s: 15 SAGA_STUCK
            // citing the run's own terminal event). An ended run awaits nothing.
            if (timeline != null && timeline.endedAt().isPresent()) continue;
            List<SagaEvent> inOrder = timeline == null ? List.of() : timeline.inBusinessOrder();
            List<String> awaited = awaitedSteps(workflow, inOrder);
            int timeoutSeconds = silenceTimeoutSeconds(workflow, awaited);
            String timeoutSource = awaited.isEmpty()
                    ? "config:anomaly.detection.default-step-timeout-seconds"
                    : "workflow:" + String.join("|", awaited) + ".timeout";

            Instant lastHeardAt = lastHeardAt(lastEvent);
            // EXT-STUCK-CITES-ARRIVAL: the clock starts at the latest arrival, but the finding is
            // about where the run stopped, so it cites the latest observation the workflow places
            // in business order -- the one the awaited step follows. Citing the latest arrival
            // pinned FTGO accounting-pause on CREATE_TICKET, whose command was merely read a
            // second after AUTHORIZE_REQUESTED although it happened before it.
            SagaEvent cited = lastPlaced(workflow, inOrder).orElse(lastEvent);
            if (lastHeardAt.plus(timeoutSeconds, ChronoUnit.SECONDS).isBefore(now)) {
                boolean alreadyDetected = anomalyRepository.findStandingBySagaId(saga.getSagaId())
                        .stream()
                        .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK
                                && a.getDetectedAt().isAfter(lastHeardAt));
                if (!alreadyDetected) {
                    createAnomaly(saga.getSagaId(), AnomalyType.SAGA_STUCK,
                            String.format("Saga stuck: nothing heard since %s (last: %s, "
                                            + "stamped %s, timeout: %ds)",
                                    lastHeardAt, cited.getEventType(),
                                    cited.getTimestamp(), timeoutSeconds),
                            details()
                                    .observedValue(String.format("nothing heard since %s, after %s",
                                            lastHeardAt, cited.getEventType()))
                                    .expectedValue(String.format("next event within %ds", timeoutSeconds))
                                    .thresholdSource(timeoutSource)
                                    .evidenceEvents(cited)
                                    .measuredAt(now));
                }
            }
        }
    }


    /**
     * The steps the run waits on after its latest observation the workflow can place.
     * Observations it cannot place say nothing about which step is in progress. Falls back
     * to that observation's own step when its outcome leads to no further step.
     */
    private List<String> awaitedSteps(WorkflowSpec workflow, List<SagaEvent> inOrder) {
        return lastPlaced(workflow, inOrder).map(event -> {
            String type = event.getEventType();
            List<String> next = workflow.stepsAwaitedAfter(type);
            return next.isEmpty() ? List.of(workflow.getExpectedStepForEvent(type)) : next;
        }).orElse(List.of());
    }

    /** The latest observation, in business order, that the workflow places at a step. */
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

    /**
     * EXT-OVERLOAD-FALSE-FINDINGS: withdraws a SAGA_STUCK whose silence the run never had.
     *
     * <p>Silence is measured on the platform's clock, which is right for a source that has
     * gone quiet and wrong for an observer that is behind: under backlog the next event sits
     * unread in Kafka while the run is reported stuck (bench 1000 ev/s: 39 standing on runs
     * that completed within a second). Only an observation read after the finding was made
     * can settle it, and it does by the source's own stamps: if the earliest-stamped of those
     * happened within the awaited step's timeout of the latest-stamped one the finding had in
     * hand, the source never went quiet -- the reading did. A duration between two events of
     * one run is what the source clock is right for. A real stall (stopped or paused service,
     * R06/R25) shows the gap in the source stamps too and stays. Events read before the
     * finding prove nothing about the silence that followed them (FTGO kitchen-stop: a
     * neighbour already in hand withdrew a real 64 s stall).
     */
    public void withdrawDisprovenSilence(WorkflowSpec workflow, String sagaId, List<SagaEvent> inOrder) {
        if (!anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.SAGA_STUCK)) return;
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

    /**
     * EXT-OVERLOAD-FALSE-FINDINGS: withdraws COMPENSATION_MISSING once a compensation start
     * is in hand. An absence claim, falsified by the event it says is absent -- which under
     * backlog is simply still unread (bench 1000 ev/s: 61 standing on COMPENSATED runs).
     */
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

    /**
     * Loads the events for a batch of sagas in one query, dropping the ones that
     * already carry {@code alreadyDetected}.
     *
     * <p>The scheduler loops used to issue two queries per saga per pass — an exists
     * check and an event fetch — so a pass cost O(batch) round trips. This makes it
     * two, regardless of batch size.
     */
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

    /**
     * Runs the configured step-timeout implementation for one saga.
     *
     * <p>Package-visible and repository-free apart from {@code createAnomaly} so the
     * legacy and projection measurements can be compared directly in tests.
     */
    void evaluateStepTimeouts(String sagaId, List<SagaEvent> events, WorkflowSpec workflow) {
        DetectionProperties.StepTimeoutMode mode = detectionProperties.getStepTimeoutMode();

        StepTimeoutFinding legacy = (mode == DetectionProperties.StepTimeoutMode.LEGACY
                || mode == DetectionProperties.StepTimeoutMode.SHADOW)
                ? findStepTimeoutByAdjacency(events, workflow)
                : null;

        StepTimeoutFinding projected = (mode == DetectionProperties.StepTimeoutMode.PROJECTION
                || mode == DetectionProperties.StepTimeoutMode.SHADOW)
                ? findStepTimeoutByProjection(sagaId, events, workflow)
                : null;

        if (mode == DetectionProperties.StepTimeoutMode.SHADOW) {
            logDivergence(sagaId, legacy, projected);
        }

        StepTimeoutFinding authoritative =
                mode == DetectionProperties.StepTimeoutMode.PROJECTION ? projected : legacy;
        if (authoritative != null) {
            createAnomaly(sagaId, AnomalyType.STEP_TIMEOUT, authoritative.description(),
                    authoritative.details());
        }
    }

    /**
     * Legacy measurement: the gap between an event and its positional predecessor.
     *
     * <p>Kept intact while SHADOW mode gathers comparison data. Its known weakness is
     * that any intervening observation — a retry event, or an event mapping to no step —
     * becomes the measurement's start point, so the reported duration of a step depends
     * on observations that are not part of that step.
     */
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

    /**
     * Projection measurement: an operation's duration from its start reference to its
     * terminal observation, with retries folded into the same operation.
     *
     * <p>Operations whose start could not be established are skipped rather than
     * measured against a substitute boundary — absence of evidence is reported as
     * absence, not as a fast step.
     */
    private StepTimeoutFinding findStepTimeoutByProjection(String sagaId, List<SagaEvent> events,
            WorkflowSpec workflow) {
        ExecutionProjection projection = executionProjectionService.project(sagaId, events, workflow);

        for (ExecutionProjection.Operation op : projection.measurableOperations()) {
            long durationMs = op.duration().toMillis();
            int expectedTimeoutMs = workflow.getStepTimeoutSeconds(op.stepId()) * 1000;
            if (durationMs <= expectedTimeoutMs) continue;

            // Two provenances, kept apart: startProvenance says which observation bounds the
            // step, the clocks say where each bound's timestamp came from (F12).
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

    /**
     * Where the two timestamps a duration spans came from (F6 records it per event).
     *
     * <p>A label, not a verdict: DECLARED→DECLARED is still two services' own clocks, and a
     * mixed or unrecorded pair can still be right. What changes is that the reader is not
     * left to assume one clock when there may be two. Tier and threshold are untouched.
     */
    static String clocks(SagaEvent from, SagaEvent to) {
        return clocks(from != null ? from.getEventTimeSource() : null,
                to != null ? to.getEventTimeSource() : null);
    }

    /** Same label, from the two origins directly; null means not recorded or not established. */
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

        // The query only narrows the candidates, so it takes the SHORTEST threshold any
        // workflow can have; each run is then held to its own workflow's. It used to apply
        // the longest step timeout across every loaded workflow to every run, so onboarding
        // a system with a 120s step moved ORDER_SAGA's threshold from 100s to 1200s (F23).
        Instant cutoff = now.minus((long) minStepTimeoutAcrossWorkflows() * multiplier, ChronoUnit.SECONDS);

        List<SagaInstance> stuckSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedBefore(
                List.of(SagaStatus.STARTED, SagaStatus.IN_PROGRESS), cutoff, detectionBatch());

        for (SagaInstance saga : stuckSagas) {
            if (saga.hasWorkflowConflict()) continue;
            int maxTimeout = maxStepTimeoutOf(specFor(saga)) * multiplier;
            if (saga.getLastUpdated() != null
                    && saga.getLastUpdated().isAfter(now.minus(maxTimeout, ChronoUnit.SECONDS))) continue;
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.MISSING_TERMINAL_EVENT)) continue;

            // EXT-OVERLOAD-FALSE-FINDINGS: the run list was read before this pass reached the
            // run; an ending ingested in between is withdrawn only by the next ending, which may
            // never come (bench 2000 ev/s: MISSING_TERMINAL_EVENT standing on 4 COMPLETED runs,
            // raised 0.1 s after their ending was read). Ask the evidence, as the stuck pass does.
            WorkflowSpec spec = specFor(saga);
            if (spec != null && EvidenceTimeline.of(spec,
                    sagaEventRepository.findBySagaIdOrderByTimestampAsc(saga.getSagaId())).endedAt().isPresent()) continue;

            long eventCount = sagaEventRepository.countBySagaId(saga.getSagaId());
            if (eventCount >= 2) {
                SagaEvent lastEvent = sagaEventRepository
                        .findTopBySagaIdOrderByTimestampDesc(saga.getSagaId())
                        .orElse(null);
                createAnomaly(saga.getSagaId(), AnomalyType.MISSING_TERMINAL_EVENT,
                        String.format("Saga has %d events but no terminal event after %ds",
                                eventCount, maxTimeout),
                        details()
                                .observedValue(String.format("%d events and no terminal event", eventCount))
                                .expectedValue(String.format("terminal event within %ds", maxTimeout))
                                .thresholdSource("config:anomaly.detection.missing-terminal-timeout-multiplier")
                                .evidenceEvents(lastEvent)
                                .measuredAt(Instant.now()));
            }
        }
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

    /**
     * Compensation that started, was declared to report completion, and has not.
     *
     * <h2>Why the declaration has to be consulted first</h2>
     * This detector used to ask one question -- was a compensation completion event seen --
     * and treat "no" as a fault. That reads an absence as evidence without first asking
     * whether anything was ever going to fill it. One of the systems validated here settles
     * the question: its {@code ROLLBACK_STOCK} declares a {@code started} event and no
     * {@code completed} one, which is not an omission but the shape of that system. Every
     * compensating run on it was therefore reported as unacknowledged, on every pass,
     * permanently -- a finding about the platform's expectation wearing the observed
     * system's name.
     *
     * <p>An absence claim needs the specification to say the event was owed (invariant
     * I10). So the question is now asked per step, and only of steps whose declaration says
     * they report completion. A step that never promised an acknowledgement cannot be
     * missing one.
     *
     * <h2>What survives</h2>
     * On a system that does declare the completion, nothing changes: a rollback that
     * started and never reported back is still reported, with the same evidence. What is
     * given up is the ability to notice a compensation stalling on a system that publishes
     * no completion at all -- and that was never observable here, because the two cases
     * look identical in the evidence. Noticing it would need something the declaration does
     * not offer, not a stricter reading of silence.
     */
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

    /**
     * Compensation starts whose step declares a completion and has not reported one.
     *
     * <p>Only the steps whose declaration owes an acknowledgement, and has not given one. A
     * step that declares no completion is dropped here, silently and on purpose: there is
     * nothing to report about a promise that was never made.
     */
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

    /**
     * EXT-ACK: withdraws MISSING_COMPENSATION_ACK once the evidence holds every
     * acknowledgement it said was missing.
     *
     * <p>An absence claim, withdrawn like the others when what it said was absent turns up.
     * Only when nothing is still owed: one late acknowledgement does not answer for another
     * compensation step that is still silent. The row is kept, with the reason, so the
     * history of the run still shows that the acknowledgement was late.
     */
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

    /** The steps a set of classified events belong to; events the workflow cannot place are dropped. */
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

            // The budget is the compensation step's own declared timeout, looked up from
            // the step the start event belongs to, rather than a fixed REFUND_PAYMENT.
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

    // ── Spec resolution and role-based event lookup ──────────────────────────
    //
    // The detectors below used to name the order saga's events directly
    // ("INVENTORY_FAILED", "REFUND_PAYMENT", "PAYMENT_REFUNDED"). They now ask the
    // saga's own workflow which of its events play those roles, so the same detector
    // works for a workflow whose compensation is releasing a reservation or reversing
    // a transfer.

    /** The specification governing this saga, or null when none is registered. */
    private WorkflowSpec specFor(SagaInstance saga) {
        return saga.hasWorkflowConflict() ? null : workflowRegistry.byType(saga.getWorkflowType()).orElse(null);
    }

    /** Events playing the given lifecycle role on any forward step. */
    private List<SagaEvent> eventsWithRole(WorkflowSpec workflow, List<SagaEvent> events,
            WorkflowSpec.EventRole.Role role) {
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(r -> r.role() == role && !workflow.isCompensationStep(r.stepId()))
                        .isPresent())
                .toList();
    }

    /** Events playing the given lifecycle role on any compensation step. */
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

    /**
     * The longest step timeout any registered workflow declares.
     *
     * <p>Used only where one cutoff has to bound a query across every workflow at once;
     * taking the maximum keeps the query from excluding sagas of the slowest workflow.
     */
    /** The slowest step of one workflow; the default timeout when it is unknown. */
    private int maxStepTimeoutOf(WorkflowSpec spec) {
        if (spec == null || spec.getSteps() == null) return detectionProperties.getDefaultStepTimeoutSeconds();
        return spec.getSteps().stream()
                .mapToInt(step -> spec.getStepTimeoutSeconds(step.getId()))
                .max()
                .orElse(detectionProperties.getDefaultStepTimeoutSeconds());
    }

    /** The smallest per-workflow threshold base, so a query bounded by it misses no run. */
    private int minStepTimeoutAcrossWorkflows() {
        return Math.min(detectionProperties.getDefaultStepTimeoutSeconds(),
                workflowRegistry.workflowNames().stream()
                        .map(name -> workflowRegistry.byType(name).orElse(null))
                        .filter(Objects::nonNull)
                        .mapToInt(this::maxStepTimeoutOf)
                        .min()
                        .orElse(detectionProperties.getDefaultStepTimeoutSeconds()));
    }

    /**
     * Row cap and ordering for one scheduler pass.
     *
     * <p>A pass must cost what the recent work costs, not what the accumulated history
     * costs. Newest-first ordering means that when a backlog exceeds the cap, the
     * sagas most likely to still be actionable are the ones examined.
     */
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
