package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.ClaimSubject;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.RcaCandidate;
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
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PromotionAttributionTest {
    private static final FailureSignalRegistry EMPTY_FAILURE_SIGNALS =
            new FailureSignalRegistry("");

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final RcaSelector selector = new RcaSelector();

    private final RcaSelector.ExecutionOrder order = new RcaSelector.ExecutionOrder() {
        private final List<String> steps =
                List.of("CREATE_ORDER", "PROCESS_PAYMENT", "RESERVE_INVENTORY");

        @Override
        public OptionalInt positionOf(String stepId) {
            int index = steps.indexOf(stepId);
            return index < 0 ? OptionalInt.empty() : OptionalInt.of(index);
        }

        @Override
        public boolean isUpstream(String candidate, String step) {
            return false;
        }
    };

    @Test
    void aClaimAboutTheObservationProcessIsRankedButNotPromoted() {
        RcaSelector.SelectionResult result = selector.select(
                List.of(outOfOrder()), List.of(anomaly(1L, AnomalyType.OUT_OF_ORDER)),
                saga(SagaStatus.COMPENSATING), order);

        assertThat(ruleIds(result)).containsExactly("R14");
        assertThat(result.getPrimary()).isNull();
    }

    @Test
    void aRunWhoseOnlyStrongCandidateIsAboutTheObserverNamesNoCause() {
        RcaSelector.SelectionResult result = selector.select(
                List.of(outOfOrder()), List.of(anomaly(1L, AnomalyType.OUT_OF_ORDER)),
                saga(SagaStatus.COMPENSATING), order);

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.INSUFFICIENT_EVIDENCE);
        assertThat(find(result, "R14").getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
        assertThat(result.getPrimary()).isNull();
    }

    @Test
    void anUnacknowledgedCompensationIsPromotedPastIt() {
        CandidateAssessment compensation = candidate(
                "R10", "COMPENSATION_STEP_FAILURE", "HIGH", CandidateStatus.STRONG_EVIDENCE);

        RcaSelector.SelectionResult result = selector.select(
                List.of(outOfOrder(), compensation),
                List.of(anomaly(1L, AnomalyType.OUT_OF_ORDER),
                        anomaly(2L, AnomalyType.MISSING_COMPENSATION_ACK)),
                saga(SagaStatus.COMPENSATING), order);

        assertThat(result.getPrimary()).isNotNull();
        assertThat(result.getPrimary().getRuleId()).isEqualTo("R10");
        assertThat(result.getPrimary().getRootCause()).isEqualTo("COMPENSATION_STEP_FAILURE");
    }

    @Test
    void aDownstreamTimeoutIsPromotedPastIt() {
        CandidateAssessment timeout = candidate(
                "R07", "DOWNSTREAM_TIMEOUT", "HIGH", CandidateStatus.STRONG_EVIDENCE);

        RcaSelector.SelectionResult result = selector.select(
                List.of(outOfOrder(), timeout),
                List.of(anomaly(1L, AnomalyType.OUT_OF_ORDER),
                        anomaly(2L, AnomalyType.STEP_TIMEOUT)),
                saga(SagaStatus.IN_PROGRESS), order);

        assertThat(result.getPrimary()).isNotNull();
        assertThat(result.getPrimary().getRuleId()).isEqualTo("R07");
    }

    @Test
    void theRankingIsUnchangedByTheEligibilityRule() {
        CandidateAssessment compensation = candidate(
                "R10", "COMPENSATION_STEP_FAILURE", "HIGH", CandidateStatus.STRONG_EVIDENCE);

        RcaSelector.SelectionResult result = selector.select(
                List.of(compensation, outOfOrder()),
                List.of(anomaly(1L, AnomalyType.OUT_OF_ORDER)),
                saga(SagaStatus.COMPENSATING), order);

        assertThat(ruleIds(result))
                .as("anchored at the first step, R14 still ranks ahead of the unanchored R10")
                .containsExactly("R14", "R10");
    }

    @Test
    void theCandidateKeepsEverythingItCarried() {
        RcaSelector.SelectionResult result = selector.select(
                List.of(outOfOrder()), List.of(anomaly(1L, AnomalyType.OUT_OF_ORDER)),
                saga(SagaStatus.COMPENSATING), order);

        RcaCandidate r14 = find(result, "R14");
        assertThat(r14.getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
        assertThat(r14.getSeverity()).isEqualTo("MEDIUM");
        assertThat(r14.getAnchorStepId()).isEqualTo("CREATE_ORDER");
        assertThat(r14.getAnchorAt()).isEqualTo(T0);
        assertThat(r14.getEvidence()).extracting(Evidence::fact)
                .containsExactly("read out of order");
        assertThat(r14.getSuggestions()).containsExactly("Check Kafka partition key assignment");
        assertThat(r14.getSubject()).isEqualTo(ClaimSubject.OBSERVATION_PROCESS);
    }

    @Test
    void aCandidateThatDeclaresNoSubjectIsStillPromotable() {
        CandidateAssessment silent = candidate(
                "R03", "MULTI_TERMINAL_CONFLICT", "CRITICAL", CandidateStatus.STRONG_EVIDENCE);

        assertThat(silent.getSubject()).isEqualTo(ClaimSubject.OBSERVED_SYSTEM);

        RcaSelector.SelectionResult result = selector.select(List.of(silent),
                List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT)),
                saga(SagaStatus.FAILED), order);

        assertThat(result.getPrimary().getRuleId()).isEqualTo("R03");
    }

    @Test
    void aFindingAboutDeliveryByTheSourceIsNotAFindingAboutTheObserver() {
        CandidateAssessment duplicate = candidate(
                "R13", "DUPLICATE_DELIVERY", "LOW", CandidateStatus.STRONG_EVIDENCE);

        assertThat(duplicate.getSubject()).isEqualTo(ClaimSubject.OBSERVED_SYSTEM);

        RcaSelector.SelectionResult result = selector.select(List.of(duplicate),
                List.of(anomaly(1L, AnomalyType.EXACT_DUPLICATE_EVENT)),
                saga(SagaStatus.COMPLETED), order);

        assertThat(result.getPrimary().getRuleId()).isEqualTo("R13");
    }

    private List<Anomaly> storedAnomalies;
    private SagaInstance analysedSaga;
    private RcaService rcaService;

    @BeforeEach
    void setUpService() {
        storedAnomalies = new ArrayList<>();
        analysedSaga = saga(SagaStatus.FAILED);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findByIdForUpdate(anyString()))
                .thenAnswer(call -> Optional.of(analysedSaga));

        SagaEventRepository sagaEvents = mock(SagaEventRepository.class);
        when(sagaEvents.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenReturn(List.of(event("ORDER_CREATED")));

        AnomalyRepository anomalyRepository = mock(AnomalyRepository.class);

        when(anomalyRepository.findStandingBySagaId(anyString()))
                .thenAnswer(call -> storedAnomalies.stream()
                        .filter(a -> a.getRetractedAt() == null)
                        .toList());

        RcaResultRepository rcaResultRepository = mock(RcaResultRepository.class);
        when(rcaResultRepository.save(any(RcaResult.class)))
                .thenAnswer(call -> call.getArgument(0));

        RuleRegistry rules = new RuleRegistry();
        rules.load();

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();

        rcaService = new RcaService(
                sagaInstances, sagaEvents, anomalyRepository, rcaResultRepository, workflows,
                new ObjectMapper(), new RcaSelector(), rules, new DetectionProperties(),
                mock(AlertSink.class), new ExecutionProjectionService(),
                EMPTY_FAILURE_SIGNALS, null);
    }

    @Test
    void theShippedOutOfOrderRuleDeclaresItselfAboutTheObservationProcess() {
        storedAnomalies.add(anomaly(1L, AnomalyType.OUT_OF_ORDER));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getCandidates())
                .as("still published, with its evidence")
                .anySatisfy(candidate -> {
                    assertThat(candidate.getRuleId()).isEqualTo("R14");
                    assertThat(candidate.getRootCause()).isEqualTo("OUT_OF_ORDER_DELIVERY");
                    assertThat(candidate.getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
                    assertThat(candidate.getSubject()).isEqualTo(ClaimSubject.OBSERVATION_PROCESS);
                    assertThat(candidate.getEvidence()).isNotEmpty();
                });
        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRuleId()).isNull();
        assertThat(result.getPrimaryRootCauses()).isEmpty();
    }

    @Test
    void aDownstreamTimeoutBesideItIsStillPromoted() {
        storedAnomalies.add(anomaly(1L, AnomalyType.OUT_OF_ORDER));
        storedAnomalies.add(anomaly(2L, AnomalyType.STEP_TIMEOUT));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getRootCause()).isEqualTo("DOWNSTREAM_TIMEOUT");
        assertThat(result.getRuleId()).isEqualTo("R07");
        assertThat(ruleIdsOf(result)).contains("R14");
    }

    @Test
    void aGenuineDuplicateDeliveryIsStillPromoted() {
        storedAnomalies.add(anomaly(1L, AnomalyType.EXACT_DUPLICATE_EVENT));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getRootCause()).isEqualTo("DUPLICATE_DELIVERY");
        assertThat(result.getRuleId()).isEqualTo("R13");
    }

    @Test
    void aGenuineInvalidTransitionIsStillPromoted() {
        storedAnomalies.add(anomaly(1L, AnomalyType.INVALID_TRANSITION));

        RcaResult result = rcaService.analyze("saga-1");

        assertThat(result.getRootCause()).isEqualTo("INVALID_TRANSITION");
        assertThat(result.getRuleId()).isEqualTo("R12");
    }

    @Test
    void retractedOperationalEvidenceStillCannotBePromoted() {
        Anomaly timeout = anomaly(2L, AnomalyType.STEP_TIMEOUT);
        storedAnomalies.add(anomaly(1L, AnomalyType.OUT_OF_ORDER));
        storedAnomalies.add(timeout);
        assertThat(rcaService.analyze("saga-1").getRuleId())
                .as("standing evidence promotes")
                .isEqualTo("R07");

        timeout.setRetractedAt(T0);
        timeout.setRetractionReason("later observations falsified this finding");

        RcaResult afterRetraction = rcaService.analyze("saga-1");

        assertThat(afterRetraction.getRootCause()).isNull();
        assertThat(afterRetraction.getRuleId()).isNull();
        assertThat(ruleIdsOf(afterRetraction))
                .as("R14 is still there, and still not the answer")
                .contains("R14");
    }

    private static CandidateAssessment outOfOrder() {
        CandidateAssessment assessment = CandidateAssessment.builder()
                .ruleId("R14")
                .rootCause("OUT_OF_ORDER_DELIVERY")
                .severity("MEDIUM")
                .status(CandidateStatus.STRONG_EVIDENCE)
                .subject(ClaimSubject.OBSERVATION_PROCESS)
                .evidence(Evidence.of("read out of order", 1L))
                .suggestion("Check Kafka partition key assignment")
                .build();
        assessment.setAnchorStepId("CREATE_ORDER");
        assessment.setAnchorAt(T0);
        return assessment;
    }

    private static CandidateAssessment candidate(String ruleId, String rootCause, String severity,
            CandidateStatus tier) {
        return CandidateAssessment.builder()
                .ruleId(ruleId)
                .rootCause(rootCause)
                .severity(severity)
                .status(tier)
                .evidence(Evidence.of("fact-" + ruleId, null))
                .build();
    }

    private static Anomaly anomaly(Long id, AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setAnomalyType(type);
        anomaly.setSagaId("saga-1");
        anomaly.setDetectedAt(T0);
        return anomaly;
    }

    private static SagaInstance saga(SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setStatus(status);
        saga.setStartTime(T0);
        return saga;
    }

    private static SagaEvent event(String eventType) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("saga-1");
        event.setEventId("e-1");
        event.setEventType(eventType);
        event.setTimestamp(T0);
        return event;
    }

    private static List<String> ruleIds(RcaSelector.SelectionResult result) {
        return result.getCandidates().stream().map(RcaCandidate::getRuleId).toList();
    }

    private static List<String> ruleIdsOf(RcaResult result) {
        return result.getCandidates().stream().map(RcaCandidate::getRuleId).toList();
    }

    private static RcaCandidate find(RcaSelector.SelectionResult result, String ruleId) {
        return result.getCandidates().stream()
                .filter(c -> ruleId.equals(c.getRuleId()))
                .findFirst().orElseThrow();
    }
}
