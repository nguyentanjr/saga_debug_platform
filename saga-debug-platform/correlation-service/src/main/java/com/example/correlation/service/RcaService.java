package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.execution.ExecutionProjection;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.ClaimSubject;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.alert.AlertEvent;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.AnomalySignatureRule;
import com.example.correlation.execution.EvidenceTimeline;
import com.example.correlation.service.rca.FailureSignal;
import com.example.correlation.service.rca.FailureSignalDeclaration;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.service.rca.ServiceDownRule;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RcaService {
    private static final Logger log = LoggerFactory.getLogger(RcaService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final WorkflowRegistry workflowRegistry;
    private final ObjectMapper objectMapper;
    private final RcaSelector rcaSelector;
    private final RuleRegistry ruleRegistry;
    private final DetectionProperties detectionProperties;
    private final AlertSink alertSink;
    private final ExecutionProjectionService executionProjectionService;
    private final FailureSignalRegistry failureSignalRegistry;
    private final RcaService self;

    private ServiceDownRule serviceDownRule;

    public RcaService(SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository,
            WorkflowRegistry workflowRegistry,
            ObjectMapper objectMapper,
            RcaSelector rcaSelector,
            RuleRegistry ruleRegistry,
            DetectionProperties detectionProperties,
            AlertSink alertSink,
            ExecutionProjectionService executionProjectionService,
            FailureSignalRegistry failureSignalRegistry,
            @Lazy RcaService self) {
        this.failureSignalRegistry = failureSignalRegistry;
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.workflowRegistry = workflowRegistry;
        this.objectMapper = objectMapper;
        this.rcaSelector = rcaSelector;
        this.ruleRegistry = ruleRegistry;
        this.detectionProperties = detectionProperties;
        this.alertSink = alertSink;
        this.executionProjectionService = executionProjectionService;
        this.self = self;
    }

    @Autowired(required = false)
    void setServiceDownRule(ServiceDownRule serviceDownRule) {
        this.serviceDownRule = serviceDownRule;
    }

    @Transactional
    public RcaResult analyze(String sagaId) {
        SagaInstance saga = sagaInstanceRepository.findByIdForUpdate(sagaId).orElse(null);
        if (saga == null) {
            log.warn("Saga {} not found for RCA", sagaId);
            return null;
        }

        rcaResultRepository.deleteAllBySagaId(sagaId);

        if (saga.hasWorkflowConflict()) {
            RcaResult refusal = RcaResult.workflowConflict(saga);
            rcaResultRepository.save(refusal);
            return refusal;
        }

        Instant evidenceReadAt = Instant.now();
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);

        List<Anomaly> anomalies = anomalyRepository.findStandingBySagaId(sagaId);

        WorkflowSpec workflow = workflowRegistry.byType(saga.getWorkflowType()).orElse(null);

        List<CandidateAssessment> candidates = new ArrayList<>();

        candidates.addAll(evaluateDeclaredRules(anomalies));

        CandidateAssessment declaredSignal = ruleDeclaredFailureSignal(workflow, events);
        addIfPresent(candidates, declaredSignal);
        addIfPresent(candidates, ruleTimeoutRetryStorm(workflow, anomalies, events));
        addIfPresent(candidates, ruleServiceCrash(saga, anomalies, events));

        var serviceDown = serviceDownRule == null ? null
                : serviceDownRule.assess(saga, events, workflow, Instant.now()).orElse(null);
        addIfPresent(candidates, ruleRecoveredAfterSilence(saga, anomalies, events));
        addIfPresent(candidates, ruleCompensationStepFailure(workflow, saga, events, anomalies));
        addIfPresent(candidates, ruleForwardStepFailure(workflow, saga, events));

        if (declaredSignal == null) {
            addIfPresent(candidates, ruleDeclaredStepFailure(workflow, saga, events));
        }
        addIfPresent(candidates, ruleOutOfOrderDelivery(saga, anomalies));

        addIfPresent(candidates, ruleStuckUnknownCause(saga, anomalies));
        addIfPresent(candidates, ruleObservabilityGap(anomalies));
        if (serviceDown != null) {
            candidates.add(serviceDown.candidate());
            ServiceDownRule.explainSymptoms(candidates, anomalies, serviceDown);
        }

        ExecutionProjection projection =
                executionProjectionService.project(sagaId, events, workflow);
        anchorCandidates(candidates, events, projection, workflow, anomalies);

        RcaSelector.SelectionResult selection =
                rcaSelector.select(candidates, anomalies, saga, executionOrder(projection, workflow));
        RcaStatus rcaStatus = selection.getRcaStatus();
        List<RcaCandidate> evidenceCandidates = evidenceCandidates(selection);

        RcaCandidate primary = selection.getPrimary();

        String outcome = determineOutcome(saga, anomalies, rcaStatus);

        List<String> evidence = new ArrayList<>();
        List<String> suggestions = new ArrayList<>();

        appendEvidenceGroup(evidence, suggestions, "Strong evidence",
                candidatesWithStatus(selection, CandidateStatus.STRONG_EVIDENCE));
        appendEvidenceGroup(evidence, suggestions, "Supporting evidence",
                candidatesWithStatus(selection, CandidateStatus.SUPPORTING_EVIDENCE));
        appendEvidenceGroup(evidence, suggestions, "Weak evidence",
                candidatesWithStatus(selection, CandidateStatus.WEAK_EVIDENCE));

        suggestions = suggestions.stream().distinct().collect(Collectors.toCollection(ArrayList::new));

        if (evidenceCandidates.isEmpty()) {
            if ("NORMAL".equals(outcome)) {
                evidence.add("Saga completed successfully with " + events.size() + " events");
                if (saga.getStartTime() != null && saga.getEndTime() != null) {
                    evidence.add(totalDuration("Total duration", saga, events));
                }
            } else {
                evidence.addAll(selection.getMissingEvidence());
                suggestions.add("Review saga events manually");
            }
        }

        appendTraceLinks(events, evidence, suggestions);

        try {
            RcaResult result = new RcaResult();
            result.setSagaId(sagaId);

            result.setRootCause(primary != null ? primary.getRootCause() : null);
            result.setOutcome(outcome);
            result.setSeverity(!evidenceCandidates.isEmpty()
                    ? highestSeverity(evidenceCandidates)
                    : fallbackSeverity(outcome, selection));

            result.setConfidence(null);
            result.setRuleId(primary != null ? primary.getRuleId() : null);
            result.setEvidence(objectMapper.writeValueAsString(evidence));
            result.setSuggestions(objectMapper.writeValueAsString(suggestions));
            result.setRcaStatus(rcaStatus);

            result.setPrimaryRootCauses(
                    primary != null && primary.getRootCause() != null
                            ? List.of(primary.getRootCause())
                            : List.of());
            result.setCandidates(selection.getCandidates());
            result.setMissingEvidence(selection.getMissingEvidence());

            if (!evidenceCandidates.isEmpty()) {
                List<String> evidenceLabels = evidenceCandidates.stream()
                        .map(cr -> cr.getRootCause() + " (" + cr.getRuleId() + ", " + cr.getStatus() + ")")
                        .collect(Collectors.toList());
                result.setContributingCauses(objectMapper.writeValueAsString(evidenceLabels));
            }

            result.setCreatedAt(evidenceReadAt);
            rcaResultRepository.save(result);
            emitAlert(result, evidence);

            log.info("RCA saga={}: rcaStatus={}, outcome={}, severity={}, rootCause={} ({}), "
                            + "evidenceCandidates={}",
                    sagaId, rcaStatus, outcome, result.getSeverity(),
                    result.getRootCause(), result.getRuleId(),
                    evidenceCandidates.stream().map(RcaCandidate::getRootCause).collect(Collectors.toList()));
            return result;
        } catch (Exception e) {
            log.error("Failed to save RCA result for saga {}: {}", sagaId, e.getMessage());
            return null;
        }
    }

    private static void appendTraceLinks(List<SagaEvent> events, List<String> evidence, List<String> suggestions) {
        List<String> traceIds = events.stream()
                .map(RcaService::traceIdOf)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (traceIds.isEmpty()) return;

        evidence.add("traceIds: " + traceIds);
        suggestions.add("View traces: " + traceIds.stream()
                .map(traceId -> "http://localhost:16686/trace/" + traceId)
                .collect(Collectors.joining(", ")));
    }

    private void anchorCandidates(List<CandidateAssessment> candidates,
            List<SagaEvent> events,
            ExecutionProjection projection,
            WorkflowSpec workflow,
            List<Anomaly> anomalies) {
        Map<String, SagaEvent> eventsById = events.stream()
                .filter(e -> e.getEventId() != null)
                .collect(Collectors.toMap(SagaEvent::getEventId, e -> e, (a, b) -> a));
        Map<Long, Anomaly> anomaliesById = anomalies.stream()
                .filter(a -> a.getId() != null)
                .collect(Collectors.toMap(Anomaly::getId, a -> a, (a, b) -> a));

        for (CandidateAssessment candidate : candidates) {
            List<SagaEvent> cited = candidate.getEvidence().stream()
                    .flatMap(evidence -> anchoringEventIds(evidence, anomaliesById, eventsById).stream())
                    .map(eventsById::get)
                    .filter(Objects::nonNull)
                    .filter(e -> e.getTimestamp() != null)
                    .toList();
            if (cited.isEmpty()) continue;

            Instant anchorAt = cited.stream().map(SagaEvent::getTimestamp)
                    .min(Comparator.naturalOrder()).orElseThrow();
            candidate.setAnchorAt(anchorAt);
            if (workflow == null) continue;

            if (candidate.getAnchorStepId() != null) continue;

            Set<Optional<String>> steps = cited.stream()
                    .filter(e -> e.getTimestamp().equals(anchorAt))
                    .map(e -> workflow.classifyEvent(e.getEventType()).map(WorkflowSpec.EventRole::stepId))
                    .collect(Collectors.toSet());
            if (steps.size() == 1) {
                steps.iterator().next()
                        .filter(stepId -> projection.operation(stepId).isPresent())
                        .ifPresent(candidate::setAnchorStepId);
            }
        }
    }

    private static final Set<AnomalyType> DURATION_FINDINGS =
            EnumSet.of(AnomalyType.STEP_TIMEOUT, AnomalyType.DELAYED_COMPENSATION);

    private static List<String> anchoringEventIds(Evidence evidence, Map<Long, Anomaly> anomaliesById,
            Map<String, SagaEvent> eventsById) {
        Anomaly anomaly = evidence.anomalyId() == null ? null : anomaliesById.get(evidence.anomalyId());
        if (anomaly == null || !DURATION_FINDINGS.contains(anomaly.getAnomalyType())) {
            return evidence.eventIds();
        }
        return evidence.eventIds().stream()
                .map(eventsById::get)
                .filter(Objects::nonNull)
                .filter(e -> e.getTimestamp() != null)
                .max(Comparator.comparing(SagaEvent::getTimestamp))
                .map(e -> List.of(e.getEventId()))
                .orElse(evidence.eventIds());
    }

    private RcaSelector.ExecutionOrder executionOrder(ExecutionProjection projection,
            WorkflowSpec workflow) {
        List<String> stepOrder = projection.operations().stream()
                .map(ExecutionProjection.Operation::stepId)
                .toList();

        Map<String, String> observedCause = new LinkedHashMap<>();
        for (ExecutionProjection.Operation op : projection.operations()) {
            if (op.startProvenance() != ExecutionProjection.StartProvenance.OBSERVED_CAUSAL_LINK) {
                continue;
            }
            projection.operations().stream()
                    .filter(other -> op.startEventId() != null
                            && op.startEventId().equals(other.endEventId()))
                    .findFirst()
                    .ifPresent(cause -> observedCause.put(op.stepId(), cause.stepId()));
        }

        return new RcaSelector.ExecutionOrder() {
            @Override
            public OptionalInt positionOf(String stepId) {
                int index = stepOrder.indexOf(stepId);
                return index < 0 ? OptionalInt.empty() : OptionalInt.of(index);
            }

            @Override
            public boolean isUpstream(String candidate, String step) {
                if (walkObservedCauses(step).contains(candidate)) {
                    return true;
                }
                return workflow != null && workflow.isUpstreamOf(candidate, step);
            }

            private Set<String> walkObservedCauses(String step) {
                Set<String> seen = new LinkedHashSet<>();
                String cursor = observedCause.get(step);
                while (cursor != null && seen.add(cursor)) {
                    cursor = observedCause.get(cursor);
                }
                return seen;
            }
        };
    }

    public List<RcaResult> getResults(String sagaId) {
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        if (saga != null && saga.hasWorkflowConflict()) return List.of(RcaResult.workflowConflict(saga));
        return rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
    }

    public void autoAnalyzeNewlyDetectedAnomalies() {
        Instant cutoff = Instant.now().minus(2, ChronoUnit.MINUTES);
        List<String> sagaIds = anomalyRepository.findSagaIdsWithAnomaliesSince(cutoff);

        for (String sagaId : sagaIds) {
            Instant latestAnomalyAt = anomalyRepository.findLatestDetectedAtBySagaId(sagaId);
            if (latestAnomalyAt == null) {
                continue;
            }

            List<RcaResult> existing = rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
            boolean needsAnalysis = existing.isEmpty()
                    || existing.get(0).getCreatedAt() == null
                    || existing.get(0).getCreatedAt().isBefore(latestAnomalyAt)
                    || citesWithdrawnFinding(existing.get(0));

            if (needsAnalysis) {
                analyzeInOwnTransaction(sagaId);
            }
        }
    }

    private boolean citesWithdrawnFinding(RcaResult result) {
        Set<Long> cited = result.getCandidates().stream()
                .flatMap(candidate -> candidate.getEvidence().stream())
                .map(Evidence::anomalyId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        if (cited.isEmpty()) return false;
        return anomalyRepository.findAllById(cited).stream().anyMatch(a -> a.getRetractedAt() != null);
    }

    public void autoAnalyzeEndedRuns() {
        PageRequest batch = PageRequest.of(0, detectionProperties.getDetectionBatchSize());
        sagaInstanceRepository.findSagaIdsAwaitingAnalysis(ENDED_WITHOUT_SUCCESS, batch)
                .forEach(this::analyzeInOwnTransaction);
        sagaInstanceRepository.findSagaIdsWithFindingsAwaitingAnalysis(List.of(SagaStatus.COMPLETED), batch)
                .forEach(this::analyzeInOwnTransaction);
    }

    private static final List<SagaStatus> ENDED_WITHOUT_SUCCESS =
            List.of(SagaStatus.FAILED, SagaStatus.COMPENSATED);

    private void analyzeInOwnTransaction(String sagaId) {
        try {
            RcaResult result = self.analyze(sagaId);
            if (result != null) {
                log.info("Auto-RCA triggered for saga {} with rcaStatus={} rootCause={}",
                        sagaId, result.getRcaStatus(), result.getRootCause());
            }
        } catch (Exception e) {
            log.error("Auto-RCA failed for saga {}: {}", sagaId, e.getMessage());
        }
    }

    private void emitAlert(RcaResult result, List<String> evidenceSummary) {
        if (result.getRcaStatus() != RcaStatus.EVIDENCE_AVAILABLE) {
            return;
        }
        if (!hasActionableEvidenceAboutTheObservedSystem(result.getCandidates())) {
            return;
        }

        AlertEvent alert = new AlertEvent(
                Instant.now(),
                result.getSagaId(),
                result.getRcaStatus().name(),
                List.copyOf(result.getPrimaryRootCauses()),
                result.getSeverity(),
                evidenceSummary != null ? List.copyOf(evidenceSummary) : List.of());

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    writeAlert(alert);
                }
            });
        } else {
            writeAlert(alert);
        }
    }

    private void writeAlert(AlertEvent alert) {
        try {
            alertSink.emit(alert);
        } catch (Exception e) {
            log.error("Failed to emit RCA alert for saga {}: {}", alert.sagaId(), e.getMessage());
        }
    }

    private List<SagaEvent> compensationEvents(WorkflowSpec workflow, List<SagaEvent> events,
            WorkflowSpec.EventRole.Role role) {
        if (workflow == null) return List.of();
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(r -> r.role() == role && workflow.isCompensationStep(r.stepId()))
                        .isPresent())
                .collect(Collectors.toList());
    }

    private static String eventTypes(List<SagaEvent> events) {
        return events.stream().map(SagaEvent::getEventType).distinct().collect(Collectors.joining(", "));
    }

    private List<CandidateAssessment> evaluateDeclaredRules(List<Anomaly> anomalies) {
        List<CandidateAssessment> matched = new ArrayList<>();

        for (AnomalySignatureRule rule : ruleRegistry.rules()) {
            List<Anomaly> matches = rule.matching(anomalies);
            if (matches.isEmpty()) continue;
            CandidateAssessment candidate = candidate(
                    rule.getRootCause(), rule.getSeverity(), rule.getId(), rule.getEvidence());

            matches.forEach(anomaly -> candidate.getEvidence().add(evidence(anomaly)));
            rule.getEvidenceNotes().forEach(note -> candidate.getEvidence().add(evidence(note)));
            candidate.getSuggestions().addAll(rule.getSuggestions());

            matched.add(candidate);
        }
        return matched;
    }

    private void addIfPresent(List<CandidateAssessment> list, CandidateAssessment candidate) {
        if (candidate != null) list.add(candidate);
    }

    private String determineOutcome(SagaInstance saga, List<Anomaly> anomalies, RcaStatus rcaStatus) {
        boolean hasAnomalies = !anomalies.isEmpty();
        boolean hasRca = rcaStatus == RcaStatus.EVIDENCE_AVAILABLE;
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED
                || saga.getStatus() == SagaStatus.FAILED;

        if (saga.getStatus() == SagaStatus.COMPLETED && !hasAnomalies) return "NORMAL";
        if (saga.getStatus() == SagaStatus.COMPLETED && hasAnomalies) return "SELF_HEALED";
        if (saga.getStatus() == SagaStatus.COMPENSATED) return "COMPENSATED";

        if (saga.getStatus() == SagaStatus.FAILED) return "FAILED";
        if (saga.getStatus() == SagaStatus.COMPENSATING) return "INCONSISTENT";

        if (hasAnomalies) return "STILL_STUCK";
        return "IN_PROGRESS";
    }

    private CandidateAssessment ruleDeclaredFailureSignal(WorkflowSpec workflow, List<SagaEvent> events) {
        if (workflow == null) return null;
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);
        for (SagaEvent event : events) {
            if (timeline.isAfterEnding(event)) continue;
            boolean isFailure = workflow.classifyEvent(event.getEventType())
                    .filter(role -> role.role() == WorkflowSpec.EventRole.Role.FAILED)
                    .isPresent();
            if (!isFailure) continue;

            FailureSignalDeclaration declaration =
                    failureSignalRegistry.forSource(event.getSourceName()).orElse(null);
            if (declaration == null) continue;

            String reason = declaration.readReason(event.getPayload()).orElse(null);
            if (reason == null) continue;

            String breakerPattern =
                    declaration.matchedPattern(FailureSignal.BREAKER_REJECTED_CALL, reason).orElse(null);
            if (breakerPattern != null) {
                CandidateAssessment rr = candidate("CIRCUIT_BREAKER_OPEN", "CRITICAL", "R01",
                        CandidateStatus.STRONG_EVIDENCE);
                rr.getEvidence().add(Evidence.ofEvent(
                        event.getEventType() + " carries the source's own report that a breaker "
                                + "refused the call",
                        reason, "the step succeeds or reports a business failure",
                        event.getEventId()));
                rr.getEvidence().add(Evidence.of(
                        "Matched declared BREAKER_REJECTED_CALL wording for source '"
                                + event.getSourceName() + "': \"" + breakerPattern + "\"", null));

                rr.setStatusReason("The source reported breaker refusal for this call. Why the "
                        + "breaker was open is not established by this evidence.");
                rr.getSuggestions().add("Check downstream service health");
                rr.getSuggestions().add("Look at what was failing before the breaker opened");
                return rr;
            }

            String exhaustedPattern =
                    declaration.matchedPattern(FailureSignal.RETRY_EXHAUSTED, reason).orElse(null);
            if (exhaustedPattern != null) {
                CandidateAssessment rr = candidate("RETRY_EXHAUSTED", "HIGH", "R23",
                        CandidateStatus.STRONG_EVIDENCE);
                rr.getEvidence().add(Evidence.ofEvent(
                        event.getEventType() + " reports that the declared retry policy was "
                                + "exhausted",
                        reason, "the step succeeds or reports a business failure",
                        event.getEventId()));
                rr.getEvidence().add(Evidence.of(
                        "Matched declared RETRY_EXHAUSTED wording for source '"
                                + event.getSourceName() + "': \"" + exhaustedPattern + "\"", null));

                rr.setStatusReason("Retries were exhausted. Nothing in this failure message "
                        + "reports breaker state, so no claim is made about one.");
                rr.getSuggestions().add("Check downstream service health");
                rr.getSuggestions().add("Review the retry policy against the step's real latency");
                return rr;
            }
        }
        return null;
    }

    private CandidateAssessment ruleTimeoutRetryStorm(WorkflowSpec workflow, List<Anomaly> anomalies,
            List<SagaEvent> events) {
        Set<String> retryEventTypes = workflow != null ? workflow.retryEventTypes() : Set.of();
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.RETRY_BURST) {
                Map<SagaEvent, String> timeoutReasons = new LinkedHashMap<>();
                for (SagaEvent e : events) {
                    if (!retryEventTypes.contains(e.getEventType())) continue;
                    failureSignalRegistry.forSource(e.getSourceName()).ifPresent(declaration ->
                            declaration.readReason(e.getPayload())
                                    .filter(reason -> declaration
                                            .matchedPattern(FailureSignal.CALL_TIMED_OUT, reason).isPresent())
                                    .ifPresent(reason -> timeoutReasons.put(e, reason)));
                }
                List<SagaEvent> timeoutRetryEvents = new ArrayList<>(timeoutReasons.keySet());
                boolean hasTimeoutEvidence = !timeoutRetryEvents.isEmpty();
                CandidateAssessment rr = candidate("TIMEOUT_RETRY_STORM", "CRITICAL", "R02",
                        hasTimeoutEvidence ? CandidateStatus.STRONG_EVIDENCE : CandidateStatus.WEAK_EVIDENCE);
                rr.getEvidence().add(evidence(a));
                if (hasTimeoutEvidence) {
                    rr.getEvidence().add(Evidence.of(
                            "Retry events carry the source's declared timeout wording",
                            timeoutReasons.values().stream()
                                    .limit(3)
                                    .collect(Collectors.joining("; ")),
                            "retry burst should not be caused by timeout exceptions",
                            null,
                            timeoutRetryEvents.stream()
                                    .map(SagaEvent::getEventId)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList())));
                } else {
                    rr.setStatusReason("No retry event carries a reason this source declares as a timeout.");
                }
                rr.getSuggestions().add("Reduce retry count or increase backoff");
                rr.getSuggestions().add("Check service logs for timeout root cause");
                return rr;
            }
        }
        return null;
    }

    private CandidateAssessment ruleServiceCrash(SagaInstance saga, List<Anomaly> anomalies,
            List<SagaEvent> events) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean hasMissingTerminal = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT);
        boolean hasStepTimeout = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.STEP_TIMEOUT);

        if (hasStuck && hasMissingTerminal && !hasStepTimeout) {
            boolean isRecovered = saga.getStatus() == SagaStatus.COMPLETED
                    || saga.getStatus() == SagaStatus.COMPENSATED
                    || saga.getStatus() == SagaStatus.FAILED;

            var workflow = workflowRegistry.byType(saga.getWorkflowType()).orElse(null);

            int maxTimeout = detectionProperties.getDefaultStepTimeoutSeconds();
            if (workflow != null && workflow.getSteps() != null) {
                maxTimeout = workflow.getSteps().stream()
                        .mapToInt(s -> workflow.getStepTimeoutSeconds(s.getId()))
                        .max().orElse(detectionProperties.getDefaultStepTimeoutSeconds());
            }
            int serviceCrashTimeoutSeconds = maxTimeout * detectionProperties.getServiceCrashTimeoutMultiplier();

            Instant stuckMeasurePoint = anomalies.stream()
                    .filter(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK
                            || a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT)
                    .map(Anomaly::getDetectedAt)
                    .filter(Objects::nonNull)
                    .max(Instant::compareTo)
                    .orElse(Instant.now());
            Instant stuckStart = events.stream()
                    .map(e -> e.getReceivedAt() != null ? e.getReceivedAt() : e.getTimestamp())
                    .filter(Objects::nonNull)
                    .filter(heard -> !heard.isAfter(stuckMeasurePoint))
                    .max(Instant::compareTo)
                    .orElse(null);
            if (stuckStart != null) {
                long stuckSeconds = Duration.between(stuckStart, stuckMeasurePoint).getSeconds();

                if (stuckSeconds > serviceCrashTimeoutSeconds) {
                    CandidateAssessment rr = candidate("SERVICE_CRASH",
                            isRecovered ? "MEDIUM" : "CRITICAL", "R05",
                            CandidateStatus.WEAK_EVIDENCE);
                    rr.setStatusReason("Only silence was observed; whether the source crashed, "
                            + "was unreachable, or its events were delayed is not established.");
                    rr.getEvidence().add(evidence("Saga stuck for " + stuckSeconds + "s (>"
                            + serviceCrashTimeoutSeconds + "s threshold)"));
                    rr.getEvidence().add(evidence("Both SAGA_STUCK and MISSING_TERMINAL_EVENT anomalies present"));
                    if (isRecovered) {
                        rr.getEvidence().add(evidence("Service recovered → saga now " + saga.getStatus()));
                    }
                    rr.getSuggestions().add("Check if downstream service container is running");
                    rr.getSuggestions().add("Verify docker container health and restart policy");
                    return rr;
                }
            }
        }
        return null;
    }

    private static String totalDuration(String label, SagaInstance saga, List<SagaEvent> events) {
        long ms = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
        return label + ": " + ms + "ms; " + AnomalyDetectionService.clocks(
                clockAt(saga.getStartTime(), events), clockAt(saga.getEndTime(), events));
    }

    private static EventTimeSource clockAt(Instant instant, List<SagaEvent> events) {
        Set<EventTimeSource> sources = events.stream()
                .filter(e -> instant.equals(e.getTimestamp()))
                .map(SagaEvent::getEventTimeSource)
                .collect(Collectors.toSet());
        return sources.size() == 1 ? sources.iterator().next() : null;
    }

    private CandidateAssessment ruleRecoveredAfterSilence(SagaInstance saga, List<Anomaly> anomalies,
            List<SagaEvent> events) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean hasMissingTerminal = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT);
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED
                || saga.getStatus() == SagaStatus.FAILED;
        boolean hasStepTimeoutBeforeRecovery = hasStepTimeoutBeforeRecovery(saga, anomalies);

        if (hasStuck && !hasMissingTerminal && !hasStepTimeoutBeforeRecovery && isTerminal) {
            CandidateAssessment rr = candidate("RECOVERED_AFTER_SILENCE", "LOW", "R06",
                    CandidateStatus.STRONG_EVIDENCE);
            rr.getEvidence().add(evidence(
                    "No observation arrived for this run for longer than the declared "
                            + "threshold (SAGA_STUCK)"));
            rr.getEvidence().add(evidence(
                    "The run was later observed to end → final status: " + saga.getStatus()));
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                rr.getEvidence().add(evidence(
                        totalDuration("Total duration including the silent period", saga, events)));
            }

            rr.setStatusReason("Silence was observed and so was the ending. Whether the "
                    + "source recovered, was restarted, or was reachable all along while "
                    + "its events arrived late is not established by this evidence.");
            rr.getSuggestions().add("Check whether the step's service restarted during the gap");
            rr.getSuggestions().add("Compare event time against receive time to see whether "
                    + "the delay was in the source or in delivery");
            return rr;
        }
        return null;
    }

    private boolean hasStepTimeoutBeforeRecovery(SagaInstance saga, List<Anomaly> anomalies) {
        if (saga.getEndTime() == null) {
            return anomalies.stream().anyMatch(a -> a.getAnomalyType() == AnomalyType.STEP_TIMEOUT);
        }
        return anomalies.stream()
                .filter(a -> a.getAnomalyType() == AnomalyType.STEP_TIMEOUT)
                .map(Anomaly::getMeasuredAt)
                .filter(Objects::nonNull)
                .anyMatch(measuredAt -> measuredAt.isBefore(saga.getEndTime()));
    }

    private CandidateAssessment ruleCompensationStepFailure(WorkflowSpec workflow, SagaInstance saga,
            List<SagaEvent> events, List<Anomaly> anomalies) {
        List<SagaEvent> started = compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.STARTED);
        boolean completed = !compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.COMPLETED).isEmpty();
        boolean hasMissingCompAck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_COMPENSATION_ACK);

        if (!started.isEmpty() && !completed
                && (saga.getStatus() == SagaStatus.COMPENSATING || hasMissingCompAck)) {
            CandidateAssessment rr = candidate("COMPENSATION_STEP_FAILURE", "HIGH", "R10",
                    CandidateStatus.STRONG_EVIDENCE);
            String startedTypes = eventTypes(started);

            rr.getEvidence().add(Evidence.of(
                    "Compensation started (" + startedTypes + ") but never acknowledged",
                    null, "the compensation reports its declared completion event", null,
                    eventIds(started)));
            rr.getEvidence().add(evidence("Saga status: " + saga.getStatus()));

            if (!hasMissingCompAck) {
                rr.setStatus(CandidateStatus.WEAK_EVIDENCE);
                rr.setStatusReason("No acknowledgement deadline has elapsed yet; the compensation may still be in flight.");
            }
            rr.getSuggestions().add("Check the compensating service's logs");
            rr.getSuggestions().add("Verify broker connectivity for the compensation topic");
            return rr;
        }
        return null;
    }

    private CandidateAssessment ruleDeclaredStepFailure(WorkflowSpec workflow, SagaInstance saga,
            List<SagaEvent> events) {
        if (workflow == null || saga.getStatus() != SagaStatus.FAILED) return null;
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);
        for (SagaEvent event : events) {
            if (timeline.isAfterEnding(event)) continue;
            boolean isFailure = workflow.classifyEvent(event.getEventType())
                    .filter(role -> role.role() == WorkflowSpec.EventRole.Role.FAILED)
                    .isPresent();
            if (!isFailure) continue;
            CandidateAssessment rr = candidate("STEP_REPORTED_FAILURE", "MEDIUM", "R24",
                    CandidateStatus.STRONG_EVIDENCE);
            rr.getEvidence().add(Evidence.ofEvent(
                    event.getEventType() + " is the failure event this workflow declares for its step",
                    event.getPayload(), "the step reports its declared completion event",
                    event.getEventId()));
            rr.setStatusReason("The step reported failure. The event carries no reason, so "
                    + "why it failed is not established by this evidence.");
            rr.getSuggestions().add("Check the failing step's own business rule and data");
            return rr;
        }
        return null;
    }

    private CandidateAssessment ruleForwardStepFailure(WorkflowSpec workflow, SagaInstance saga,
            List<SagaEvent> events) {
        List<SagaEvent> started = compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.STARTED);
        List<SagaEvent> completed = compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.COMPLETED);

        Optional<SagaEvent> forwardFailure = forwardFailures(workflow, events).stream().findFirst();
        if (saga.getStatus() == SagaStatus.COMPENSATED && !started.isEmpty() && !completed.isEmpty()
                && forwardFailure.isPresent()) {
            CandidateAssessment rr = candidate("FORWARD_STEP_FAILURE", "LOW", "R11",
                    CandidateStatus.STRONG_EVIDENCE);

            forwardFailure.ifPresent(failed ->
                    rr.getEvidence().add(Evidence.ofEvent(
                            failed.getEventType() + " is the forward step's declared failure",
                            null, "the step reports its declared completion event",
                            failed.getEventId())));
            rr.getEvidence().add(Evidence.of("Compensation completed: "
                    + eventTypes(started) + " -> " + eventTypes(completed), null, null, null,
                    eventIds(completed)));
            rr.getEvidence().add(evidence("Saga status: COMPENSATED"));
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                rr.getEvidence().add(evidence(
                        totalDuration("Total duration (including compensation)", saga, events)));
            }
            rr.getSuggestions().add("Saga rolled back correctly");
            rr.getSuggestions().add("Review why the forward step failed to prevent recurrence");
            return rr;
        }
        return null;
    }

    private CandidateAssessment ruleOutOfOrderDelivery(SagaInstance saga, List<Anomaly> anomalies) {
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED;
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.OUT_OF_ORDER) {
                CandidateAssessment rr = candidate("OUT_OF_ORDER_DELIVERY",
                        isTerminal ? "LOW" : "MEDIUM", "R14",
                        CandidateStatus.STRONG_EVIDENCE);
                rr.setSubject(ClaimSubject.OBSERVATION_PROCESS);
                rr.getEvidence().add(evidence(a));
                if (isTerminal) {
                    rr.getEvidence().add(evidence("Saga completed despite out-of-order events"));
                } else {
                    rr.getEvidence().add(evidence("Saga still in " + saga.getStatus() + " status"));
                }

                rr.getSuggestions().add("Nothing indicated in the source: this is the order the observer "
                        + "read the events in, and Kafka does not order separate topics against each other");
                rr.getSuggestions().add("Read the run by event time, not by arrival");
                return rr;
            }
        }
        return null;
    }

    private CandidateAssessment ruleObservabilityGap(List<Anomaly> anomalies) {
        if (!anomalies.isEmpty()) {
            CandidateAssessment rr = candidate("OBSERVABILITY_GAP", "LOW", "R20",
                    CandidateStatus.WEAK_EVIDENCE);
            rr.getEvidence().add(evidence("Anomalies detected but insufficient evidence to determine root cause"));
            rr.getEvidence().add(evidence("Anomaly types: " + anomalies.stream()
                    .map(a -> a.getAnomalyType().name())
                    .distinct()
                    .collect(Collectors.joining(", "))));
            rr.getSuggestions().add("Add more telemetry sources for better RCA");
            rr.getSuggestions().add("Check if Jaeger/logs provide additional context");
            return rr;
        }
        return null;
    }

    private CandidateAssessment ruleStuckUnknownCause(SagaInstance saga, List<Anomaly> anomalies) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean isNonTerminal = saga.getStatus() != SagaStatus.COMPLETED
                && saga.getStatus() != SagaStatus.COMPENSATED
                && saga.getStatus() != SagaStatus.FAILED;

        if (hasStuck && isNonTerminal) {
            CandidateAssessment rr = candidate("STUCK_UNKNOWN_CAUSE", "HIGH", "R21",
                    CandidateStatus.WEAK_EVIDENCE);
            rr.getEvidence().add(evidence("Saga stuck in " + saga.getStatus() + " status"));
            rr.getEvidence().add(evidence("No specific root cause identified — insufficient evidence"));
            rr.getSuggestions().add("Check all downstream service health");
            rr.getSuggestions().add("Verify Kafka consumer group offsets");
            return rr;
        }
        return null;
    }

    private CandidateAssessment candidate(String rootCause, String severity, String ruleId,
            CandidateStatus tier) {
        CandidateAssessment candidate = CandidateAssessment.builder()
                .rootCause(rootCause)
                .severity(severity)
                .ruleId(ruleId)
                .status(tier)
                .build();
        if (tier == CandidateStatus.WEAK_EVIDENCE) {
            candidate.setStatusReason(
                    "Weak evidence only; treat " + rootCause + " as a fallback hypothesis.");
        }
        return candidate;
    }

    private static List<String> eventIds(List<SagaEvent> events) {
        return events.stream().map(SagaEvent::getEventId).filter(Objects::nonNull).toList();
    }

    private static List<SagaEvent> forwardFailures(WorkflowSpec workflow, List<SagaEvent> events) {
        if (workflow == null) return List.of();
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(role -> role.role() == WorkflowSpec.EventRole.Role.FAILED
                                && !workflow.isCompensationStep(role.stepId()))
                        .isPresent())
                .toList();
    }

    private Evidence evidence(String fact) {
        return Evidence.of(fact, null);
    }

    private Evidence evidence(Anomaly anomaly) {
        if (anomaly == null) return null;
        return Evidence.of(
                anomaly.getDescription(),
                anomaly.getObservedValue(),
                anomaly.getExpectedValue(),
                anomaly.getId(),
                anomaly.getEvidenceEventIds());
    }

    private List<RcaCandidate> candidatesWithStatus(RcaSelector.SelectionResult selection, CandidateStatus status) {
        return selection.getCandidates().stream()
                .filter(candidate -> candidate.getStatus() == status)
                .collect(Collectors.toList());
    }

    private List<RcaCandidate> evidenceCandidates(RcaSelector.SelectionResult selection) {
        return selection.getCandidates().stream()
                .filter(candidate -> isEvidenceStatus(candidate.getStatus()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private boolean isEvidenceStatus(CandidateStatus status) {
        return status == CandidateStatus.STRONG_EVIDENCE
                || status == CandidateStatus.SUPPORTING_EVIDENCE
                || status == CandidateStatus.WEAK_EVIDENCE;
    }

    private void appendEvidenceGroup(List<String> evidence,
            List<String> suggestions,
            String groupLabel,
            List<RcaCandidate> groupCandidates) {
        if (groupCandidates == null || groupCandidates.isEmpty()) {
            return;
        }
        for (RcaCandidate candidate : groupCandidates) {
            List<String> facts = evidenceFacts(candidate);
            String factSummary = facts.isEmpty() ? "No structured evidence facts recorded" : String.join("; ", facts);
            String position = candidate.getAnchorStepId() != null
                    ? " @" + candidate.getAnchorStepId()
                    : "";
            String precededBy = candidate.getPrecededBy().isEmpty()
                    ? ""
                    : " (downstream of " + String.join(", ", candidate.getPrecededBy()) + ")";
            evidence.add(String.format("[%s] %s (%s)%s%s: %s",
                    groupLabel, candidate.getRootCause(), candidate.getRuleId(),
                    position, precededBy, factSummary));
            suggestions.addAll(candidate.getSuggestions());
        }
    }

    private int statusRank(CandidateStatus status) {
        if (status == CandidateStatus.STRONG_EVIDENCE) return 0;
        if (status == CandidateStatus.SUPPORTING_EVIDENCE) return 1;
        if (status == CandidateStatus.WEAK_EVIDENCE) return 2;
        if (status == CandidateStatus.RULED_OUT) return 3;
        return 4;
    }

    private List<String> evidenceFacts(RcaCandidate candidate) {
        return candidate.getEvidence().stream()
                .map(Evidence::fact)
                .filter(fact -> fact != null && !fact.isBlank())
                .collect(Collectors.toList());
    }

    private static boolean hasActionableEvidenceAboutTheObservedSystem(List<RcaCandidate> candidates) {
        return candidates != null && candidates.stream()
                .anyMatch(candidate -> candidate.getSubject() == ClaimSubject.OBSERVED_SYSTEM
                        && (candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE
                                || candidate.getStatus() == CandidateStatus.SUPPORTING_EVIDENCE));
    }

    private String highestSeverity(List<RcaCandidate> candidates) {
        return candidates.stream()
                .map(RcaCandidate::getSeverity)
                .filter(Objects::nonNull)
                .max(Comparator.comparingInt(this::severityRank))
                .orElse("LOW");
    }

    private int severityRank(String severity) {
        if ("CRITICAL".equals(severity)) return 4;
        if ("HIGH".equals(severity)) return 3;
        if ("MEDIUM".equals(severity)) return 2;
        if ("LOW".equals(severity)) return 1;
        return 0;
    }

    private String fallbackSeverity(String outcome, RcaSelector.SelectionResult selection) {
        if ("NORMAL".equals(outcome)) return "NONE";
        return highestSeverity(selection.getCandidates());
    }

    private static String traceIdOf(SagaEvent event) {
        if (event == null) return null;
        String traceId = event.getTraceId();
        return traceId != null && !traceId.isBlank() ? traceId : null;
    }
}
