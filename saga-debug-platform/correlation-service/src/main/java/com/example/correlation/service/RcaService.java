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
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.OptionalInt;
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
    /** Optional: absent in tests that construct this service by hand, and then R25 is not assessed. */
    private com.example.correlation.service.rca.ServiceDownRule serviceDownRule;

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

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    void setServiceDownRule(com.example.correlation.service.rca.ServiceDownRule serviceDownRule) {
        this.serviceDownRule = serviceDownRule;
    }

    @Transactional
    public RcaResult analyze(String sagaId) {
        // Locks the saga for this transaction so two concurrent analyses cannot both
        // delete the previous result and both insert a new one.
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

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        // Only the findings that still stand. A withdrawn claim is one the platform has
        // said later observations falsified; reasoning from it publishes as a diagnosis
        // something already known to be wrong about the run.
        List<Anomaly> anomalies = anomalyRepository.findStandingBySagaId(sagaId);

        
        WorkflowSpec workflow = workflowRegistry.byType(saga.getWorkflowType()).orElse(null);

        List<CandidateAssessment> candidates = new ArrayList<>();
        // Declared rules first: R03, R04, R07, R09, R12, R13, R15, R19, R22 live in
        // rules/*.yaml, because a set of anomaly types is their entire condition.
        candidates.addAll(evaluateDeclaredRules(anomalies));

        // Procedural rules: each needs something a signature match cannot express —
        // payload content, saga status, timestamp arithmetic, or a missing event.
        CandidateAssessment declaredSignal = ruleDeclaredFailureSignal(workflow, events);
        addIfPresent(candidates, declaredSignal);
        addIfPresent(candidates, ruleTimeoutRetryStorm(workflow, anomalies, events));
        addIfPresent(candidates, ruleServiceCrash(saga, anomalies, events));
        // R25: the one rule that reads beyond the run's own events. When the runtime says the
        // awaited step's service was down, the timeout and silence it caused are explained.
        var serviceDown = serviceDownRule == null ? null
                : serviceDownRule.assess(saga, events, workflow, Instant.now()).orElse(null);
        addIfPresent(candidates, ruleRecoveredAfterSilence(saga, anomalies, events));
        addIfPresent(candidates, ruleCompensationStepFailure(workflow, saga, events, anomalies));
        addIfPresent(candidates, ruleForwardStepFailure(workflow, saga, events));
        // Only when R01/R23 read nothing: they cite the same failed event more specifically,
        // and a second claim on it would compete for promotion on severity alone.
        if (declaredSignal == null) {
            addIfPresent(candidates, ruleDeclaredStepFailure(workflow, saga, events));
        }
        addIfPresent(candidates, ruleOutOfOrderDelivery(saga, anomalies));

        
        
        addIfPresent(candidates, ruleStuckUnknownCause(saga, anomalies));
        addIfPresent(candidates, ruleObservabilityGap(anomalies));
        if (serviceDown != null) {
            candidates.add(serviceDown.candidate());
            com.example.correlation.service.rca.ServiceDownRule.explainSymptoms(candidates, anomalies, serviceDown);
        }

        // Locate each candidate in the execution before ranking, so ordering reflects
        // where the execution diverged rather than how the rules happen to be named.
        ExecutionProjection projection =
                executionProjectionService.project(sagaId, events, workflow);
        anchorCandidates(candidates, events, projection, workflow, anomalies);

        RcaSelector.SelectionResult selection =
                rcaSelector.select(candidates, anomalies, saga, executionOrder(projection, workflow));
        RcaStatus rcaStatus = selection.getRcaStatus();
        List<RcaCandidate> evidenceCandidates = evidenceCandidates(selection);
        // The candidate the selector ranked first among the strongly-evidenced ones, or
        // null when it found none. Read rather than recomputed: re-deriving a winner here
        // would be a second ranking that could disagree with the one in the candidate list.
        RcaCandidate primary = selection.getPrimary();
        // Order already established by the selector, which knows where each candidate
        // sits in the execution. Re-sorting here on severity and rule id would throw
        // that away and put the alphabetically-first rule back on top.

        
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

        

        
        List<String> traceIds = events.stream()
                .map(RcaService::traceIdOf)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
        if (!traceIds.isEmpty()) {
            evidence.add("traceIds: " + traceIds);
            suggestions.add("View traces: " + traceIds.stream()
                    .map(tid -> "http://localhost:16686/trace/" + tid)
                    .collect(Collectors.joining(", ")));
        }

        
        try {
            RcaResult result = new RcaResult();
            result.setSagaId(sagaId);
            // The reported cause is the promoted candidate's, and null when nothing was
            // promoted. These three fields were pinned to null when the score-free model
            // replaced the old single-primary one, so a run could rank a STRONG_EVIDENCE
            // candidate, publish it in `candidates`, and still report no root cause at all
            // — the diagnosis was reached and then not stated. Every field below still
            // comes from a candidate the rules assessed; nothing is inferred here.
            result.setRootCause(primary != null ? primary.getRootCause() : null);
            result.setOutcome(outcome);
            result.setSeverity(!evidenceCandidates.isEmpty()
                    ? highestSeverity(evidenceCandidates)
                    : fallbackSeverity(outcome, selection));
            // Deliberately still null: the score-free model removed confidence scores, and
            // promoting a candidate is not grounds for inventing one. See V5.
            result.setConfidence(null);
            result.setRuleId(primary != null ? primary.getRuleId() : null);
            result.setEvidence(objectMapper.writeValueAsString(evidence));
            result.setSuggestions(objectMapper.writeValueAsString(suggestions));
            result.setRcaStatus(rcaStatus);
            // Mirrors root_cause as a list, which is the shape V5 established when it
            // introduced the column (primary_root_causes = jsonb_build_array(root_cause)).
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

    /**
     * Ties each candidate to the step its earliest piece of evidence belongs to.
     *
     * <p>Evidence already carries the event ids the rule relied on; the projection says
     * which operation each of those events belongs to. Taking the earliest gives the
     * point in the execution where this candidate's story starts, which is what makes
     * "which of these came first" answerable at all.
     *
     * <p>Without a cited event carrying a timestamp, a candidate keeps a null anchor and
     * sorts last. An anomaly's measurement time alone is not used as an execution anchor.
     */
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
            // A rule that established its step from its own evidence keeps it. R25 knows the
            // step the run was waiting on; the event it cites is the one that ended the wait,
            // which for a source without per-step completions belongs to a later step, and
            // re-anchoring there put every such crash one step late (EXT-R25-MISS-KITCHEN).
            if (candidate.getAnchorStepId() != null) continue;

            // Every cited event at the earliest instant, not the first one the stream met:
            // when they belong to different steps the evidence does not say which step the
            // story starts in, and the step stays unknown rather than being picked (F11).
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

    /** Findings that measure how long the run took to reach an event (F21, F25). */
    private static final Set<AnomalyType> DURATION_FINDINGS =
            EnumSet.of(AnomalyType.STEP_TIMEOUT, AnomalyType.DELAYED_COMPENSATION);

    /**
     * The events an evidence item places in the execution.
     *
     * <p>Usually every event it cites. A duration finding is the exception: it cites the event
     * that opened the interval and the one that closed it, and what it says is late is the
     * step of the closing one — the step that took too long to report. Anchoring it at the
     * opening event put a RESERVE_INVENTORY timeout at PROCESS_PAYMENT (F25), and put a late
     * refund at the inventory failure that triggered it, level with that failure, where
     * severity then chose the delay over the failure (F21).
     */
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

    /**
     * Execution position and dependency direction.
     *
     * <p>Dependency is answered from what was observed where the telemetry supports it,
     * and from the specification otherwise. The distinction matters: the specification
     * says which steps <em>can</em> depend on which, while the causal links say which
     * ones <em>did</em> in this run. A step that the spec places downstream but that no
     * observed chain connects to is not evidence of anything here.
     */
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

    /**
     * Not transactional, and deliberately so: analysis of each saga runs in its own
     * transaction via {@code self}. Wrapping the whole sweep in one transaction meant a
     * single failing saga rolled back every result the pass had already produced, and
     * held one long-lived transaction open across the entire batch.
     */
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
                    || existing.get(0).getCreatedAt().isBefore(latestAnomalyAt);

            if (needsAnalysis) {
                analyzeInOwnTransaction(sagaId);
            }
        }
    }

    /**
     * L17 — runs that ended without success and hold evidence newer than their last
     * analysis. The pass above is driven by findings, and a run that ended by its step's
     * own declared failure (R24) or by a completed compensation (R11) usually has none, so
     * it was analysed only when someone called {@code /rca/analyze}. The analysis itself is
     * unchanged: a FAILED run with nothing to cite still comes out NO_DIAGNOSIS_NEEDED.
     *
     * <p>COMPLETED runs are included only when they carry findings. The pass above re-runs a
     * diagnosis when a new finding appears, which left one shape behind: a run diagnosed
     * while it waited, that then completed without raising anything new — a source with no
     * per-step completion events gives no step timeout to raise. Its diagnosis was made on
     * part of its evidence and stayed so (FTGO pause runs: R06 never assessed). A healthy
     * completion carries no findings and is still not analysed.
     */
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
            // Through the proxy, so @Transactional applies per saga.
            RcaResult result = self.analyze(sagaId);
            if (result != null) {
                log.info("Auto-RCA triggered for saga {} with rcaStatus={} rootCause={}",
                        sagaId, result.getRcaStatus(), result.getRootCause());
            }
        } catch (Exception e) {
            log.error("Auto-RCA failed for saga {}: {}", sagaId, e.getMessage());
        }
    }

    /**
     * An alert announces the stored result and says no more than it does.
     *
     * <p>It used to list every STRONG, SUPPORTING and WEAK candidate as
     * {@code primaryRootCauses}. On a live run of the hotel saga the stored result named no
     * cause — its only strong candidate was R14, a claim about this platform's own reading
     * order ({@link ClaimSubject#OBSERVATION_PROCESS}) — while the alert for the same result
     * named {@code OUT_OF_ORDER_DELIVERY} as the primary root cause.
     *
     * <p>So the causes are the result's own promoted ones, and an alert is raised only when
     * there is actionable evidence about the observed system. A run whose only evidence is
     * about the observation process raises none.
     */
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

        // Deferred to commit. Emitting inline announced a diagnosis that the enclosing
        // transaction could still roll back, leaving an alert on disk for a result that
        // was never stored.
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

    /** Events playing the given lifecycle role on any compensation step of this workflow. */
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

    /**
     * Runs every declared anomaly-signature rule against this saga's anomalies.
     *
     * <p>Order of the returned candidates carries no meaning: {@code RcaSelector} ranks
     * them by evidence tier and by where they sit in the execution, so which file a rule
     * was declared in cannot influence which diagnosis wins. That was not true of the
     * previous implementation, where the order of {@code addIfPresent} calls decided
     * ties.
     */
    private List<CandidateAssessment> evaluateDeclaredRules(List<Anomaly> anomalies) {
        List<CandidateAssessment> matched = new ArrayList<>();

        for (AnomalySignatureRule rule : ruleRegistry.rules()) {
            List<Anomaly> matches = rule.matching(anomalies);
            if (matches.isEmpty()) continue;
            CandidateAssessment candidate = candidate(
                    rule.getRootCause(), rule.getSeverity(), rule.getId(), rule.getEvidence());

            // The matched anomalies first: they carry the event ids that let
            // anchorCandidates() place this candidate in the execution.
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


    
    
    
    /**
     * R01 / R23 — what the source system's own failure message says, read through its
     * declaration.
     *
     * <p>The previous version matched {@code payload.contains("circuit breaker") ||
     * contains("retry exhausted")} over the whole payload and answered
     * {@code CIRCUIT_BREAKER_OPEN} at STRONG whenever either hit. Measuring SC06 on
     * 2026-09-21 showed what that cost. Across three runs of one scenario the breaker was
     * genuinely CLOSED in the first and OPEN in the other two — verified from
     * payment-service's own actuator — and the platform returned the same verdict, at the
     * same tier, citing the same evidence, all three times. Both substrings live in a
     * constant prefix the source writes on <em>every</em> retry exhaustion, so the
     * predicate could not see the difference. It was right twice for a reason that was not
     * the evidence, and wrong once.
     *
     * <p>What actually separates the runs is the <em>suffix</em>: the underlying exception's
     * message, which in two of them is the breaker's own refusal and in the other is the
     * gateway error. Reading it requires knowing which field carries the reason and what
     * that system's breaker refusal looks like — facts about the observed system, so
     * declared per source rather than compiled in (I7). A source with no declaration yields
     * no candidate here: the platform has not been told how to read its failure text, and
     * inferring one anyway is the defect, not the fix.
     *
     * <p>Two candidates, because they are two claims at two depths. Both are mechanisms:
     * neither says why the system was failing, only how this call ended.
     */
    private CandidateAssessment ruleDeclaredFailureSignal(WorkflowSpec workflow, List<SagaEvent> events) {
        if (workflow == null) return null;
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);
        for (SagaEvent event : events) {
            if (timeline.isAfterEnding(event)) continue;   // see ruleDeclaredStepFailure
            boolean isFailure = workflow.classifyEvent(event.getEventType())
                    .filter(role -> role.role() == WorkflowSpec.EventRole.Role.FAILED)
                    .isPresent();
            if (!isFailure) continue;

            FailureSignalDeclaration declaration =
                    failureSignalRegistry.forSource(event.getSourceName()).orElse(null);
            if (declaration == null) continue;

            String reason = declaration.readReason(event.getPayload()).orElse(null);
            if (reason == null) continue;

            // Breaker refusal first: it is the more specific of the two, and a message that
            // reports it also came out of an exhausted retry, so testing exhaustion first
            // would mask it.
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
                // Says what it is and what it is not. The refusal is observed; the reason
                // the breaker was open is not in this evidence, and in SC06 it was in fact
                // one layer below -- the breaker opened because the gateway kept failing.
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
                // The claim stops where the evidence does. This is the run whose message
                // says nothing about a breaker, and naming a breaker here is exactly the
                // over-reach F4 removed.
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
                // The source's own words for a timeout, read through its declaration (L3). A
                // source that declares none yields WEAK: the burst is observed, its cause is not.
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

            // Resolved from the saga's own workflow: R05's threshold is the slowest step
            // of the workflow this execution belongs to, not of whichever workflow
            // happened to be loaded first.
            var workflow = workflowRegistry.byType(saga.getWorkflowType()).orElse(null);
            
            int maxTimeout = detectionProperties.getDefaultStepTimeoutSeconds();
            if (workflow != null && workflow.getSteps() != null) {
                maxTimeout = workflow.getSteps().stream()
                        .mapToInt(s -> workflow.getStepTimeoutSeconds(s.getId()))
                        .max().orElse(detectionProperties.getDefaultStepTimeoutSeconds());
            }
            int serviceCrashTimeoutSeconds = maxTimeout * detectionProperties.getServiceCrashTimeoutMultiplier();

            // Both ends on the platform's clock (F15). detectedAt is when this platform raised
            // the findings; the start is when it last heard from the run, which is
            // receivedAt. Subtracting the source's own timestamp instead added a lagging
            // source's lag to the silence and let one running ahead hide it entirely.
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
                    
                    
                    // A hypothesis, not supporting evidence. All it rests on is silence, and
                    // silence does not tell a crashed source from a partitioned one, a stalled
                    // consumer, or an observer reading late -- the same reason F5 withdrew
                    // TRANSIENT_SERVICE_FAILURE. At SUPPORTING it made a run "evidence
                    // available" while naming nothing, once F23 let the rule fire at all.
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

    
    
    
    
    
    
    
    
    
    
    
    
    
    /**
     * R06 — a run that went quiet past the threshold and later ended.
     *
     * <p>This rule used to answer {@code TRANSIENT_SERVICE_FAILURE} at STRONG, with the
     * suggestion "Service self-healed". Its whole evidence is {@code SAGA_STUCK}, which is
     * a statement about <em>silence the observer measured</em> (L-6), plus a terminal
     * status arriving afterwards. Three different worlds produce exactly that:
     *
     * <ul>
     *   <li>the source failed and recovered by itself;</li>
     *   <li>the source was fine and the observer received its events late;</li>
     *   <li>someone restarted it.</li>
     * </ul>
     *
     * <p>The third is what actually happened in the scenarios this rule was written
     * against: SC04 and SC16 both stop a container and then start it again by hand, so the
     * conclusion "self-healed" was not merely unproven, it was false — and the platform
     * stated it at the tier reserved for evidence that settles a question.
     *
     * <p>The asymmetry gave it away. {@link #ruleStuckUnknownCause} sees the same silence
     * on a run that has <em>not</em> ended and correctly answers
     * {@code STUCK_UNKNOWN_CAUSE} at WEAK. A terminal status arriving later says the run
     * finished; it says nothing about why the silence happened. One extra observation
     * should not move a claim from "unknown cause" to a confident diagnosis of the source.
     *
     * <p>So the claim is now about the execution, which is what was observed, rather than
     * about the source's health, which was not. The tier stays STRONG because the narrower
     * claim really is settled by the evidence: the silence was measured and the ending was
     * observed. It remains promotable, so the platform still reports something useful — it
     * just stops naming a cause it cannot see.
     *
     * <p>{@code TRANSIENT_SERVICE_FAILURE} is gone rather than demoted. A tier is a
     * statement about how sure the platform is; the problem here was the <em>claim</em>,
     * and a hedged version of a claim the evidence cannot reach is still that claim. Should
     * a source report a transient failure itself, that belongs in a declared
     * {@link com.example.correlation.service.rca.FailureSignal} category, the same way F4
     * moved breaker state out of substring guessing — read from what the system says, not
     * inferred from what it failed to say.
     */
    /**
     * "<label>: <ms>ms; clocks=..." — the run's span between its first and last timestamps,
     * with where those two timestamps came from (F15, as F12 did for step timeouts). When
     * the events at a bound disagree on origin the bound's clock is not established, and
     * the label says unrecorded rather than picking one of them.
     */
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
            // Says plainly what the silence does not settle, so a reader does not supply
            // the missing half themselves.
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
            // Cites the requests that went unanswered. Without event ids this candidate had
            // no anchor and ranked below any anchored one whatever its severity -- SC23 lost
            // to the duplicate delivery of the very request it is about.
            rr.getEvidence().add(Evidence.of(
                    "Compensation started (" + startedTypes + ") but never acknowledged",
                    null, "the compensation reports its declared completion event", null,
                    eventIds(started)));
            rr.getEvidence().add(evidence("Saga status: " + saga.getStatus()));
            // Fires as soon as the saga enters COMPENSATING, which is the same event that
            // starts the compensation, so a compensation still within its budget looks
            // identical to one that failed. MISSING_COMPENSATION_ACK is the signal that
            // waited for a deadline; without it this is a hypothesis, not a failure.
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

    
    
    
    
    /**
     * R24 — the step's own declared failure event, when the source gave no reason text.
     *
     * <p>{@link #ruleDeclaredFailureSignal} needs a reason field to read, and a business
     * rejection such as {@code ROOM_REJECTED} carries only a status. Without this rule a
     * FAILED saga whose failure is a plain rejection produced no candidate at all and was
     * reported as NORMAL. The claim is kept to what the event establishes: this step
     * reported failure. Why it did so is not in the evidence and is not asserted.
     */
    private CandidateAssessment ruleDeclaredStepFailure(WorkflowSpec workflow, SagaInstance saga,
            List<SagaEvent> events) {
        if (workflow == null || saga.getStatus() != SagaStatus.FAILED) return null;
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);
        for (SagaEvent event : events) {
            // A failure reported after the run had already ended did not end it: that is a
            // write after the ending (R03/R04's evidence), and citing it here put a second
            // claim on the same event that won on step position (SC21, 2026-09-29).
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

        if (saga.getStatus() == SagaStatus.COMPENSATED && !started.isEmpty() && !completed.isEmpty()) {
            CandidateAssessment rr = candidate("FORWARD_STEP_FAILURE", "LOW", "R11",
                    CandidateStatus.STRONG_EVIDENCE);
            // The forward failure the compensation answered is this claim's subject, so it is
            // what the candidate is anchored at; unanchored, R11 ranked below every anchored
            // symptom of the rollback itself (SC24).
            forwardFailures(workflow, events).stream().findFirst().ifPresent(failed ->
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

    
    

    
    

    
    
    
    
    /**
     * What OUT_OF_ORDER establishes, and what it does not.
     *
     * <p>The detector behind it sorts a run's observations by when this platform received
     * them and reports a pair whose event times run the other way. That is a true and
     * useful statement about the reception order, and it stays a candidate with all its
     * evidence. It is not a statement that the observed system violated an ordering
     * contract: the pairs it reports come from different topics, and Kafka defines no
     * ordering across topics for either side to have broken. It appears on backlog and
     * multi-topic catch-up reads, where this platform is choosing the order, and not on
     * live ingestion or on replay of records it already holds.
     *
     * <p>So the claim's subject is the observation process, and it is declared as such —
     * which keeps it out of promotion without touching its tier, its severity, its anchor
     * or its rank. See {@link com.example.correlation.domain.rca.ClaimSubject}.
     */
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
                // About the reading, like the claim (L15). Pointing at the source's partition
                // keys sent readers to a fault the evidence does not show.
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

    /** Declared FAILED events of forward (non-compensation) steps. */
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

    /** Same bar as the selector's EVIDENCE_AVAILABLE, narrowed to claims about the observed system. */
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

    /**
     * The trace this event belongs to, as its source's mapping said to read it.
     *
     * <p>Was a search of the payload for a field named {@code traceId} — this platform's
     * own name — so a system keeping it anywhere else produced a report with no link to
     * its traces. The value now arrives through {@code SourceMapping.traceId} like every
     * other mapped field.
     */
    private static String traceIdOf(SagaEvent event) {
        if (event == null) return null;
        String traceId = event.getTraceId();
        return traceId != null && !traceId.isBlank() ? traceId : null;
    }
}
