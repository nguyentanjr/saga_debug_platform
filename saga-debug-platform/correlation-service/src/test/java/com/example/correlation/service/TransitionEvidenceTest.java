package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpec.TransitionJudgment;
import com.example.correlation.workflow.WorkflowSpec.TransitionVerdict;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransitionEvidenceTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    private Map<String, SagaInstance> instances;
    private List<SagaEvent> events;
    private List<Anomaly> anomalies;

    private WorkflowRegistry registry;
    private WorkflowSpec orderSaga;
    private WorkflowSpec loanApproval;
    private CorrelationService correlationService;
    private SagaInstanceRepository sagaInstanceRepository;
    private SagaEventRepository sagaEventRepository;
    private AnomalyRepository anomalyRepository;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        events = new ArrayList<>();
        anomalies = new ArrayList<>();

        sagaInstanceRepository = mock(SagaInstanceRepository.class);
        when(sagaInstanceRepository.findById(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstanceRepository.findByIdForUpdate(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(call -> {
            SagaInstance instance = call.getArgument(0);
            instances.put(instance.getSagaId(), instance);
            return instance;
        });

        sagaEventRepository = mock(SagaEventRepository.class);
        when(sagaEventRepository.findBySourceNameAndEventId(anyString(), anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getEventId().equals(call.getArgument(1)))
                        .findFirst());
        when(sagaEventRepository.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getSagaId().equals(call.getArgument(0)))
                        .sorted(Comparator.comparing(SagaEvent::getTimestamp))
                        .toList());
        when(sagaEventRepository.save(any(SagaEvent.class))).thenAnswer(call -> {
            events.add(call.getArgument(0));
            return call.getArgument(0);
        });

        anomalyRepository = mock(AnomalyRepository.class);
        when(anomalyRepository.save(any(Anomaly.class))).thenAnswer(call -> {
            Anomaly anomaly = call.getArgument(0);
            if (anomaly.getId() == null) {
                anomaly.setId((long) anomalies.size() + 1);
                anomalies.add(anomaly);
            }
            return anomaly;
        });
        when(anomalyRepository.findStandingBySagaId(anyString())).thenAnswer(call ->
                anomalies.stream().filter(a -> a.getSagaId().equals(call.getArgument(0))
                        && !a.isRetracted()).toList());
        when(anomalyRepository.existsBySagaIdAndAnomalyType(anyString(), any()))
                .thenAnswer(call -> anomalies.stream()
                        .anyMatch(a -> a.getSagaId().equals(call.getArgument(0))
                                && a.getAnomalyType() == call.getArgument(1) && !a.isRetracted()));

        registry = new WorkflowRegistry();
        registry.load();
        orderSaga = registry.byType("ORDER_SAGA").orElseThrow();
        loanApproval = registry.byType("LOAN_APPROVAL").orElseThrow();

        AnomalyDetectionService anomalyDetectionService = new AnomalyDetectionService(
                sagaInstanceRepository, sagaEventRepository, anomalyRepository,
                registry, new ObjectMapper(), new DetectionProperties(),
                new ExecutionProjectionService());

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    @Test
    void aTargetReachableThroughAnUnobservedStepIsNotJudged() {
        TransitionJudgment judgment = orderSaga.explainTransition(
                "CREATE_ORDER", "RESERVE_INVENTORY", WorkflowSpec.SUCCESS_OUTCOME);

        assertThat(judgment.verdict()).isEqualTo(TransitionVerdict.UNKNOWN);
        assertThat(judgment.stepsInBetween()).containsExactly("PROCESS_PAYMENT");
    }

    @Test
    void aMissingIntermediateObservationRaisesNoInvalidTransition() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void severalMissingStepsInARowAreStillJustMissing() {
        assertThat(loanApproval.explainTransition(
                "SUBMIT_APPLICATION", "DISBURSE_FUNDS", WorkflowSpec.SUCCESS_OUTCOME))
                .extracting(TransitionJudgment::verdict)
                .isEqualTo(TransitionVerdict.UNKNOWN);

        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l4", "FUNDS_DISBURSED", 3);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void anIntermediateEventArrivingLateCompletesTheRunWithoutAFinding() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e2", "PAYMENT_COMPLETED", 1);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    @Test
    void anOutcomeRoutedSomewhereItCannotReachIsInvalid() {
        assertThat(loanApproval.judgeTransition("ASSESS_RISK", "HUMAN_REVIEW", "APPROVED"))
                .isEqualTo(TransitionVerdict.INVALID);

        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_APPROVED", 1);
        observe("l3", "REVIEW_APPROVED", 2);

        assertThat(raised()).contains(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void aRunMovingBackwardsThroughItsGraphIsInvalid() {
        assertThat(orderSaga.judgeTransition(
                "PROCESS_PAYMENT", "CREATE_ORDER", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.INVALID);
    }

    @Test
    void aStepFollowingAnOutcomeThatEndsTheRunIsInvalid() {
        assertThat(orderSaga.judgeTransition(
                "RESERVE_INVENTORY", "CREATE_ORDER", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.INVALID);
    }

    @Test
    void aDeclaredTransitionIsValid() {
        assertThat(orderSaga.judgeTransition(
                "CREATE_ORDER", "PROCESS_PAYMENT", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
        assertThat(orderSaga.judgeTransition(
                "PROCESS_PAYMENT", "RESERVE_INVENTORY", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
    }

    @Test
    void aHealthyRunRaisesNoTransitionFinding() {
        observeHappyPath();

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void aHealthyRunOfAWorkflowWithNamedOutcomesRaisesNoTransitionFinding() {
        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_APPROVED", 1);
        observe("l3", "FUNDS_DISBURSED", 2);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void theBranchingRunOfThatWorkflowRaisesNoTransitionFindingEither() {
        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_FLAGGED", 1);
        observe("l3", "REVIEW_APPROVED", 2);
        observe("l4", "FUNDS_DISBURSED", 3);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void redeliveryDoesNotChangeTheVerdict() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);

        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e1", "ORDER_CREATED", 0);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(events).hasSize(2);
    }

    @Test
    void redeliveryDoesNotDuplicateATrueFinding() {
        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_APPROVED", 1);
        observe("l3", "REVIEW_APPROVED", 2);
        observe("l3", "REVIEW_APPROVED", 2);

        assertThat(raised().stream().filter(t -> t == AnomalyType.INVALID_TRANSITION).count())
                .isEqualTo(1);
    }

    @Test
    void noDeliveryOrderOfAHealthyRunProducesAFinding() {
        List<String[]> happyPath = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "PAYMENT_COMPLETED", "1"},
                new String[]{"e3", "INVENTORY_RESERVED", "2"});

        for (List<String[]> order : permutations(happyPath)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .doesNotContain(AnomalyType.INVALID_TRANSITION);
        }
    }

    @Test
    void aTrulyInvalidRunIsFoundInEveryDeliveryOrder() {
        List<String[]> invalidRun = List.of(
                new String[]{"l1", "APPLICATION_SUBMITTED", "0"},
                new String[]{"l2", "ASSESSMENT_APPROVED", "1"},
                new String[]{"l3", "REVIEW_APPROVED", "2"});

        for (List<String[]> order : permutations(invalidRun)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .contains(AnomalyType.INVALID_TRANSITION);
        }
    }

    @Test
    void aFailureHandingOffToItsCompensationIsValid() {
        assertThat(orderSaga.judgeTransition(
                "RESERVE_INVENTORY", "REFUND_PAYMENT", WorkflowSpec.FAILED_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
        assertThat(loanApproval.judgeTransition(
                "DISBURSE_FUNDS", "REVERSE_DISBURSEMENT", WorkflowSpec.FAILED_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
    }

    @Test
    void aCompensationReachedThroughUnobservedStepsIsNotJudged() {
        assertThat(orderSaga.judgeTransition(
                "CREATE_ORDER", "REFUND_PAYMENT", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    @Test
    void aCompensatingRunReadBackwardsRaisesNoTransitionFinding() {
        observe("c5", "PAYMENT_REFUNDED", 4);
        observe("c4", "REFUND_PAYMENT", 3);
        observe("c3", "INVENTORY_FAILED", 2);
        observe("c2", "PAYMENT_COMPLETED", 1);
        observe("c1", "ORDER_CREATED", 0);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    void anEventAfterATerminalStateIsStillReported() {
        observeHappyPath();
        observe("e9", "INVENTORY_FAILED", 9);

        assertThat(raised()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void anEventThatOnlyArrivedLateIsStillNotReported() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e2", "PAYMENT_COMPLETED", 1);

        assertThat(raised()).doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void aPostTerminalEventIsFoundInEveryDeliveryOrder() {
        List<String[]> run = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "INVENTORY_RESERVED", "10"},
                new String[]{"e3", "PAYMENT_FAILED", "20"});

        for (List<String[]> order : permutations(run)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
        }
    }

    @Test
    void anObservationAfterTheEndingIsNotAlsoJudgedAsAHandoff() {
        List<String[]> run = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "PAYMENT_COMPLETED", "1"},
                new String[]{"e3", "INVENTORY_RESERVED", "2"},
                new String[]{"e4", "PAYMENT_COMPLETED", "3"});

        for (List<String[]> order : permutations(run)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[0]).toList())
                    .contains(AnomalyType.FORBIDDEN_POST_TERMINAL)
                    .doesNotContain(AnomalyType.INVALID_TRANSITION);
        }
    }

    @Test
    void noDeliveryOrderOfARunWithNoPostTerminalEventRaisesOne() {
        List<String[]> run = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "PAYMENT_COMPLETED", "1"},
                new String[]{"e3", "INVENTORY_RESERVED", "2"});

        for (List<String[]> order : permutations(run)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
        }
    }

    @Test
    void aPostTerminalFindingIsRaisedInEveryOrderWhenSeveralPairsQualify() {
        assertThat(postTerminalShapesOverEveryOrderOf(severalQualifyingPairs()))
                .as("orders that raised no finding")
                .doesNotContainKey(NO_FINDING);
    }

    @Test
    void everyDeliveryOrderCitesTheSamePostTerminalPair() {
        assertThat(postTerminalShapesOverEveryOrderOf(severalQualifyingPairs()))
                .containsOnlyKeys("[e3, e4] @" + T0.plusSeconds(3));
    }

    @Test
    void aLateEarlierEndingRevisesTheSameOccurrenceAndItsWholeExplanation() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e4", "PAYMENT_FAILED", 3);
        observe("e5", "INVENTORY_FAILED", 4);
        Anomaly finding = postTerminalFinding();
        Long id = finding.getId();
        assertThat(finding.getEvidenceEventIds()).containsExactly("e4", "e5");
        finding.setDetectedAt(T0);

        observe("e3", "INVENTORY_RESERVED", 2);

        assertThat(postTerminalFinding()).isSameAs(finding);
        assertThat(finding.getId()).isEqualTo(id);
        assertThat(finding.getEvidenceEventIds()).containsExactly("e3", "e4");
        assertThat(finding.getMeasuredAt()).isEqualTo(T0.plusSeconds(3));
        assertThat(finding.getDescription()).isEqualTo(
                "Event 'PAYMENT_FAILED' at 2026-01-01T00:00:03Z happened after "
                        + "'INVENTORY_RESERVED' ended the saga as COMPLETED at 2026-01-01T00:00:02Z");
        assertThat(finding.getObservedValue()).isEqualTo("PAYMENT_FAILED after terminal status COMPLETED");
        assertThat(finding.getExpectedValue()).isEqualTo("no events after terminal saga status");
        assertThat(finding.getThresholdSource()).isEqualTo("code:terminal-status-guard");
        assertThat(finding.getDetectedAt()).isAfter(T0);
        assertThat(finding.isRetracted()).isFalse();
    }

    @Test
    void anEarlierPostTerminalObservationRevisesTheMeasurementToo() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e5", "INVENTORY_FAILED", 4);
        Anomaly finding = postTerminalFinding();
        assertThat(finding.getEvidenceEventIds()).containsExactly("e3", "e5");

        observe("e4", "PAYMENT_FAILED", 3);

        assertThat(postTerminalFinding()).isSameAs(finding);
        assertThat(finding.getEvidenceEventIds()).containsExactly("e3", "e4");
        assertThat(finding.getMeasuredAt()).isEqualTo(T0.plusSeconds(3));
    }

    @Test
    void anUnchangedPairAndADuplicateDoNotRewriteOrRedetectTheFinding() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e4", "PAYMENT_FAILED", 3);
        Anomaly finding = postTerminalFinding();
        Instant detected = finding.getDetectedAt();
        clearInvocations(anomalyRepository);

        observe("e2", "PAYMENT_COMPLETED", 1);
        observe("e4", "PAYMENT_FAILED", 3);

        assertThat(postTerminalFinding()).isSameAs(finding);
        assertThat(finding.getDetectedAt()).isEqualTo(detected);
        assertThat(events).hasSize(4);
        verify(anomalyRepository, never()).save(finding);
    }

    @Test
    void tiedEndingsChooseADeterministicCitationWithoutClaimingOrderBetweenThem() {
        List<String[]> run = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e3b", "INVENTORY_RESERVED", "2"},
                new String[]{"e3a", "INVENTORY_RESERVED", "2"},
                new String[]{"e4", "PAYMENT_FAILED", "3"});
        assertThat(postTerminalShapesOverEveryOrderOf(run))
                .containsOnlyKeys("[e3a, e4] @" + T0.plusSeconds(3));

        assertThat(postTerminalShapesOverEveryOrderOf(run.subList(0, 3)))
                .containsOnlyKeys(NO_FINDING);
    }

    @Test
    void thePostTerminalCitationGivesTheSameRcaAnchorRankingAndPrimaryInEveryOrder() {
        RuleRegistry rules = new RuleRegistry();
        rules.load();
        for (List<String[]> order : permutations(severalQualifyingPairs())) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            Anomaly conflict = new Anomaly();
            conflict.setSagaId(RUN.canonical());
            conflict.setAnomalyType(AnomalyType.MULTI_TERMINAL_CONFLICT);
            conflict.setDescription("COMPLETED and FAILED terminal observations");
            conflict.setEvidenceEventIds(List.of("e3", "e5"));
            conflict.setMeasuredAt(T0.plusSeconds(4));
            when(anomalyRepository.findStandingBySagaId(RUN.canonical()))
                    .thenReturn(List.of(postTerminalFinding(), conflict));
            RcaService rca = rcaService(rules, mock(RcaResultRepository.class), null);

            RcaResult result = rca.analyze(RUN.canonical());

            RcaCandidate candidate = result.getCandidates().stream()
                    .filter(c -> "R04".equals(c.getRuleId())).findFirst().orElseThrow();
            assertThat(candidate.getAnchorAt()).isEqualTo(T0.plusSeconds(2));
            assertThat(candidate.getAnchorStepId()).isEqualTo("RESERVE_INVENTORY");
            assertThat(result.getCandidates().stream().map(RcaCandidate::getRuleId).toList())
                    .containsSubsequence("R03", "R04");
            assertThat(result.getRuleId()).isEqualTo("R03");
        }
    }

    @Test
    void revisedEvidenceMakesTheExistingAutoRcaSweepReanalyzeButAnUnchangedPairDoesNot() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e4", "PAYMENT_FAILED", 3);
        observe("e5", "INVENTORY_FAILED", 4);
        Anomaly finding = postTerminalFinding();
        finding.setDetectedAt(T0);
        RcaResult previous = new RcaResult();
        previous.setCreatedAt(T0.plusSeconds(10));
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.findBySagaIdOrderByCreatedAtDesc(RUN.canonical())).thenReturn(List.of(previous));
        when(anomalyRepository.findSagaIdsWithAnomaliesSince(any())).thenAnswer(call ->
                finding.getDetectedAt().isBefore(call.getArgument(0)) ? List.of() : List.of(RUN.canonical()));
        when(anomalyRepository.findLatestDetectedAtBySagaId(RUN.canonical()))
                .thenAnswer(call -> finding.getDetectedAt());
        RcaService analysis = mock(RcaService.class);
        RcaService sweep = rcaService(new RuleRegistry(), results, analysis);

        sweep.autoAnalyzeNewlyDetectedAnomalies();
        verify(analysis, never()).analyze(anyString());
        observe("e3", "INVENTORY_RESERVED", 2);
        sweep.autoAnalyzeNewlyDetectedAnomalies();
        verify(analysis).analyze(RUN.canonical());

        previous.setCreatedAt(finding.getDetectedAt().plusSeconds(1));
        clearInvocations(analysis);
        observe("e2", "PAYMENT_COMPLETED", 1);
        sweep.autoAnalyzeNewlyDetectedAnomalies();
        verify(analysis, never()).analyze(anyString());
    }

    private RcaService rcaService(RuleRegistry rules, RcaResultRepository results, RcaService self) {
        return new RcaService(sagaInstanceRepository, sagaEventRepository, anomalyRepository,
                results, registry, new ObjectMapper(), new RcaSelector(), rules,
                new DetectionProperties(), alert -> { }, new ExecutionProjectionService(),
                new FailureSignalRegistry(""), self);
    }

    private Anomaly postTerminalFinding() {
        List<Anomaly> findings = anomalies.stream()
                .filter(a -> a.getAnomalyType() == AnomalyType.FORBIDDEN_POST_TERMINAL).toList();
        assertThat(findings).hasSize(1);
        return findings.get(0);
    }

    private static final String NO_FINDING = "NONE";

    private static List<String[]> severalQualifyingPairs() {
        return List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "PAYMENT_COMPLETED", "1"},
                new String[]{"e3", "INVENTORY_RESERVED", "2"},
                new String[]{"e4", "PAYMENT_FAILED", "3"},
                new String[]{"e5", "INVENTORY_FAILED", "4"});
    }

    @Test
    void everyDeliveryOrderCitesTheSameStateRegressionPair() {
        assertThat(shapesOverEveryOrderOf(severalQualifyingPairs(), AnomalyType.STATE_REGRESSION))
                .hasSize(1)
                .doesNotContainKey(NO_FINDING);
    }

    @Test
    void everyDeliveryOrderCitesTheSameConflictingTerminals() {
        assertThat(shapesOverEveryOrderOf(severalQualifyingPairs(), AnomalyType.MULTI_TERMINAL_CONFLICT))
                .containsOnlyKeys("[e3, e4] @" + T0.plusSeconds(3));
    }

    @Test
    void theOrderFindingsAreReadBackInDoesNotChangeTheDiagnosis() {
        severalQualifyingPairs().forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
        List<Anomaly> standing = anomalies.stream().filter(a -> !a.isRetracted()).toList();
        assertThat(standing).extracting(Anomaly::getAnomalyType)
                .contains(AnomalyType.FORBIDDEN_POST_TERMINAL, AnomalyType.STATE_REGRESSION);
        RuleRegistry rules = new RuleRegistry();
        rules.load();

        Set<String> diagnoses = new java.util.HashSet<>();
        for (List<Anomaly> order : permutations(standing)) {
            when(anomalyRepository.findStandingBySagaId(RUN.canonical())).thenReturn(order);
            RcaResult result = rcaService(rules, mock(RcaResultRepository.class), null).analyze(RUN.canonical());
            diagnoses.add(result.getRootCause() + " " + result.getEvidence() + " "
                    + result.getCandidates().stream()
                            .map(c -> c.getRuleId() + "@" + c.getAnchorAt() + "/" + c.getAnchorStepId())
                            .toList());
        }
        assertThat(diagnoses).hasSize(1);
    }

    @Test
    void anInvalidHandoffIsFiledOnceHoweverOftenItIsReJudged() {
        for (List<String[]> order : permutations(severalQualifyingPairs())) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            List<String> handoffs = anomalies.stream()
                    .filter(a -> a.getAnomalyType() == AnomalyType.INVALID_TRANSITION && !a.isRetracted())
                    .map(a -> a.getObservedValue() + " " + a.getEvidenceEventIds())
                    .toList();
            assertThat(handoffs).as("order %s", order.stream().map(e -> e[0]).toList())
                    .doesNotHaveDuplicates();
        }
    }

    private Map<String, List<String>> postTerminalShapesOverEveryOrderOf(List<String[]> run) {
        return shapesOverEveryOrderOf(run, AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    private Map<String, List<String>> shapesOverEveryOrderOf(List<String[]> run, AnomalyType type) {
        Map<String, List<String>> shapes = new LinkedHashMap<>();
        for (List<String[]> order : permutations(run)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            String shape = anomalies.stream()
                    .filter(a -> a.getAnomalyType() == type && !a.isRetracted())
                    .findFirst()
                    .map(a -> a.getEvidenceEventIds() + " @" + a.getMeasuredAt())
                    .orElse(NO_FINDING);
            shapes.computeIfAbsent(shape, key -> new ArrayList<>())
                    .add(order.stream().map(e -> e[0]).toList().toString());
        }
        return shapes;
    }

    @Test
    void aStepTheGraphDoesNotMentionIsNotJudged() {
        assertThat(orderSaga.judgeTransition(
                "A_STEP_NO_SPEC_DECLARES", "PROCESS_PAYMENT", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    @Test
    void aStepHandingOffToItselfIsNotATransition() {
        assertThat(orderSaga.judgeTransition(
                "RESERVE_INVENTORY", "RESERVE_INVENTORY", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    private SagaInstance run() {
        return instances.get(RUN.canonical());
    }

    private List<AnomalyType> raised() {
        return anomalies.stream().map(Anomaly::getAnomalyType).toList();
    }

    private void observeHappyPath() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e2", "PAYMENT_COMPLETED", 1);
        observe("e3", "INVENTORY_RESERVED", 2);
    }

    private static <T> List<List<T>> permutations(List<T> items) {
        if (items.isEmpty()) return List.of(List.of());
        List<List<T>> all = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            List<T> rest = new ArrayList<>(items);
            T head = rest.remove(i);
            for (List<T> tail : permutations(rest)) {
                List<T> one = new ArrayList<>();
                one.add(head);
                one.addAll(tail);
                all.add(one);
            }
        }
        return all;
    }

    private void observe(String eventId, String eventType, long secondsIn) {
        correlationService.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SOURCE, RUN, eventId, null,
                        CorrelationBasis.DECLARED_KEY),
                null,
                eventType,
                "UNKNOWN",
                "a-service",
                T0.plusSeconds(secondsIn),
                "{}",
                Instant.now(),
                IngestedEvent.Causality.NONE));
    }
}
