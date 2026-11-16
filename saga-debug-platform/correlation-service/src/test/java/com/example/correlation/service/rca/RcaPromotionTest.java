package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.DetectionProperties;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RcaPromotionTest {
    private static final FailureSignalRegistry EMPTY_FAILURE_SIGNALS =
            new FailureSignalRegistry("");

    private final RcaSelector selector = new RcaSelector();

    @Test
    void aStrongCandidateIsPromotedWithItsRuleAndRootCause() {
        RcaSelector.SelectionResult result = selector.select(
                List.of(candidate("R03", "MULTI_TERMINAL_CONFLICT", CandidateStatus.STRONG_EVIDENCE)),
                List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT)),
                saga(SagaStatus.FAILED));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.EVIDENCE_AVAILABLE);
        assertThat(result.getPrimary()).isNotNull();
        assertThat(result.getPrimary().getRuleId()).isEqualTo("R03");
        assertThat(result.getPrimary().getRootCause()).isEqualTo("MULTI_TERMINAL_CONFLICT");
    }

    @Test
    void thePromotedCandidateIsTheOneInTheCandidateList() {
        RcaSelector.SelectionResult result = selector.select(
                List.of(candidate("R07", "DOWNSTREAM_TIMEOUT", CandidateStatus.STRONG_EVIDENCE)),
                List.of(anomaly(7L, AnomalyType.STEP_TIMEOUT)),
                saga(SagaStatus.IN_PROGRESS));

        assertThat(result.getCandidates()).contains(result.getPrimary());
        assertThat(result.getPrimary().getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
    }

    @Test
    void promotionFollowsTheExistingRankingRatherThanRuleId() {
        CandidateAssessment weakerButFirstAlphabetically =
                candidate("R03", "MULTI_TERMINAL_CONFLICT", CandidateStatus.WEAK_EVIDENCE);
        CandidateAssessment strong =
                candidate("R12", "INVALID_TRANSITION", CandidateStatus.STRONG_EVIDENCE);

        RcaSelector.SelectionResult result = selector.select(
                List.of(weakerButFirstAlphabetically, strong),
                List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT),
                        anomaly(2L, AnomalyType.INVALID_TRANSITION)),
                saga(SagaStatus.FAILED));

        assertThat(result.getPrimary().getRuleId()).isEqualTo("R12");
    }

    @Test
    void aRuledOutCandidateIsNotPromoted() {
        CandidateAssessment fallback =
                candidate("R20", "OBSERVABILITY_GAP", CandidateStatus.STRONG_EVIDENCE);
        CandidateAssessment specific =
                candidate("R07", "DOWNSTREAM_TIMEOUT", CandidateStatus.STRONG_EVIDENCE);

        RcaSelector.SelectionResult result = selector.select(List.of(fallback, specific),
                List.of(anomaly(7L, AnomalyType.STEP_TIMEOUT)), saga(SagaStatus.IN_PROGRESS));

        assertThat(find(result, "R20").getStatus()).isEqualTo(CandidateStatus.RULED_OUT);
        assertThat(result.getPrimary().getRuleId()).isEqualTo("R07");
    }

    @Test
    void aSelectionOfNothingButRuledOutCandidatesPromotesNothing() {
        CandidateAssessment excluded =
                candidate("R20", "OBSERVABILITY_GAP", CandidateStatus.STRONG_EVIDENCE);
        excluded.setStatus(CandidateStatus.RULED_OUT);

        RcaSelector.SelectionResult result = selector.select(List.of(excluded),
                List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT)), saga(SagaStatus.FAILED));

        assertThat(result.getPrimary()).isNull();
    }

    @Test
    void nothingWeakerThanStrongEvidenceIsPromoted() {
        RcaSelector.SelectionResult supportingOnly = selector.select(
                List.of(candidate("R05", "SLOW_STEP", CandidateStatus.SUPPORTING_EVIDENCE)),
                List.of(anomaly(5L, AnomalyType.STEP_TIMEOUT)), saga(SagaStatus.IN_PROGRESS));

        assertThat(supportingOnly.getRcaStatus()).isEqualTo(RcaStatus.EVIDENCE_AVAILABLE);
        assertThat(supportingOnly.getPrimary()).isNull();

        RcaSelector.SelectionResult weakOnly = selector.select(
                List.of(candidate("R21", "STUCK_UNKNOWN_CAUSE", CandidateStatus.WEAK_EVIDENCE)),
                List.of(anomaly(9L, AnomalyType.SAGA_STUCK)), saga(SagaStatus.IN_PROGRESS));

        assertThat(weakOnly.getRcaStatus()).isEqualTo(RcaStatus.INSUFFICIENT_EVIDENCE);
        assertThat(weakOnly.getPrimary()).isNull();
    }

    @Test
    void aSelectionWithNoCandidatesPromotesNothing() {
        RcaSelector.SelectionResult result =
                selector.select(List.of(), List.of(), saga(SagaStatus.COMPLETED));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.NO_DIAGNOSIS_NEEDED);
        assertThat(result.getPrimary()).isNull();
    }

    private AnomalyRepository anomalyRepository;
    private RcaResultRepository rcaResultRepository;
    private List<Anomaly> storedAnomalies;
    private SagaInstance analysedSaga;
    private RcaService rcaService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUpService() {
        storedAnomalies = new ArrayList<>();
        analysedSaga = saga(SagaStatus.FAILED);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findByIdForUpdate(anyString()))
                .thenAnswer(call -> Optional.of(analysedSaga));

        SagaEventRepository sagaEvents = mock(SagaEventRepository.class);
        when(sagaEvents.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenReturn(List.of(event("ORDER_CREATED")));

        anomalyRepository = mock(AnomalyRepository.class);

        when(anomalyRepository.findStandingBySagaId(anyString()))
                .thenAnswer(call -> storedAnomalies.stream()
                        .filter(a -> a.getRetractedAt() == null)
                        .toList());

        rcaResultRepository = mock(RcaResultRepository.class);
        when(rcaResultRepository.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        RuleRegistry rules = new RuleRegistry();
        rules.load();

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();

        rcaService = new RcaService(
                sagaInstances,
                sagaEvents,
                anomalyRepository,
                rcaResultRepository,
                workflows,
                new ObjectMapper(),
                new RcaSelector(),
                rules,
                new DetectionProperties(),
                mock(AlertSink.class),
                new ExecutionProjectionService(),
                EMPTY_FAILURE_SIGNALS,
                null);
    }

    @Test
    void aStrongCandidateReachesTheSavedResult() {
        storedAnomalies.add(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.EVIDENCE_AVAILABLE);
        assertThat(result.getRootCause()).isEqualTo("MULTI_TERMINAL_CONFLICT");
        assertThat(result.getRuleId()).isEqualTo("R03");
        assertThat(result.getPrimaryRootCauses()).containsExactly("MULTI_TERMINAL_CONFLICT");
    }

    @Test
    void promotionDoesNotInventAConfidenceScore() {
        storedAnomalies.add(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT));

        assertThat(rcaService.analyze("saga-1").getConfidence()).isNull();
    }

    @Test
    void retractedEvidenceCannotBecomeTheRootCause() {
        Anomaly withdrawn = anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT);
        storedAnomalies.add(withdrawn);
        assertThat(rcaService.analyze("saga-1").getRootCause())
                .as("standing evidence promotes")
                .isEqualTo("MULTI_TERMINAL_CONFLICT");

        withdrawn.setRetractedAt(Instant.parse("2026-01-01T00:00:00Z"));
        withdrawn.setRetractionReason("later observations falsified this finding");

        RcaResult afterRetraction = rcaService.analyze("saga-1");

        assertThat(afterRetraction.getRootCause()).isNull();
        assertThat(afterRetraction.getRuleId()).isNull();
        assertThat(afterRetraction.getPrimaryRootCauses()).isEmpty();
    }

    @Test
    void noQualifyingCandidateLeavesRootCauseAndRuleIdNull() {
        analysedSaga.setStatus(SagaStatus.IN_PROGRESS);
        storedAnomalies.add(anomaly(9L, AnomalyType.SAGA_STUCK));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.INSUFFICIENT_EVIDENCE);
        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRuleId()).isNull();
        assertThat(result.getPrimaryRootCauses()).isEmpty();
    }

    @Test
    void aRecoveredStuckRunPromotesTheStrongCandidateItAlreadyHad() {
        storedAnomalies.add(anomaly(9L, AnomalyType.SAGA_STUCK));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getRootCause()).isEqualTo("RECOVERED_AFTER_SILENCE");
        assertThat(result.getRuleId()).isEqualTo("R06");
    }

    @Test
    void silenceFollowedByAnEndingClaimsNothingAboutTheSourcesHealth() {
        storedAnomalies.add(anomaly(9L, AnomalyType.SAGA_STUCK));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getPrimaryRootCauses())
                .as("no promoted claim may diagnose the source from silence alone")
                .noneMatch(cause -> cause.contains("SERVICE_FAILURE")
                        || cause.contains("SELF_HEAL")
                        || cause.contains("CRASH"));
        assertThat(result.getRootCause()).isEqualTo("RECOVERED_AFTER_SILENCE");
    }

    @Test
    void outOfOrderDeliveryAdvisesAboutTheReadingNotTheSource() {
        storedAnomalies.add(anomaly(1L, AnomalyType.OUT_OF_ORDER));

        com.example.correlation.domain.rca.RcaCandidate r14 = rcaService.analyze("saga-1").getCandidates()
                .stream().filter(c -> "R14".equals(c.getRuleId())).findFirst().orElseThrow();

        assertThat(r14.getSuggestions())
                .noneMatch(s -> s.toLowerCase().contains("partition key"))
                .anyMatch(s -> s.contains("observer"));
    }

    @Test
    void aRunWithNoAnomaliesPromotesNothing() {
        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRuleId()).isNull();
        assertThat(result.getPrimaryRootCauses()).isEmpty();
    }

    private static CandidateAssessment candidate(String ruleId, String rootCause,
            CandidateStatus tier) {
        return CandidateAssessment.builder()
                .ruleId(ruleId)
                .rootCause(rootCause)
                .severity("HIGH")
                .status(tier)
                .evidence(Evidence.of("fact-" + ruleId, null))
                .build();
    }

    private static Anomaly anomaly(Long id, AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setAnomalyType(type);
        anomaly.setSagaId("saga-1");
        anomaly.setDetectedAt(Instant.parse("2026-01-01T00:00:00Z"));
        return anomaly;
    }

    private static SagaInstance saga(SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setStatus(status);
        saga.setStartTime(Instant.parse("2026-01-01T00:00:00Z"));
        return saga;
    }

    private static SagaEvent event(String eventType) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("saga-1");
        event.setEventId("e-1");
        event.setEventType(eventType);
        event.setTimestamp(Instant.parse("2026-01-01T00:00:00Z"));
        return event;
    }

    private static com.example.correlation.domain.rca.RcaCandidate find(
            RcaSelector.SelectionResult result, String ruleId) {
        return result.getCandidates().stream()
                .filter(c -> ruleId.equals(c.getRuleId()))
                .findFirst()
                .orElseThrow();
    }
}
