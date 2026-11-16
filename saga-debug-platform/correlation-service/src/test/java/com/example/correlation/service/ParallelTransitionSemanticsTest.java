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
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ParallelTransitionSemanticsTest {
    private static final FailureSignalRegistry EMPTY_FAILURE_SIGNALS =
            new FailureSignalRegistry("");

    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private WorkflowRegistry registry;
    private WorkflowSpec parallelCheckout;
    private WorkflowSpec parallelFulfilment;
    private WorkflowSpec orderSaga;
    private WorkflowSpec loanApproval;

    private Map<String, SagaInstance> instances;
    private List<SagaEvent> events;
    private List<Anomaly> anomalies;
    private CorrelationService correlationService;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        events = new ArrayList<>();
        anomalies = new ArrayList<>();

        SagaInstanceRepository sagaInstanceRepository = mock(SagaInstanceRepository.class);
        when(sagaInstanceRepository.findById(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstanceRepository.findByIdForUpdate(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(call -> {
            SagaInstance instance = call.getArgument(0);
            instances.put(instance.getSagaId(), instance);
            return instance;
        });

        SagaEventRepository sagaEventRepository = mock(SagaEventRepository.class);
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

        AnomalyRepository anomalyRepository = mock(AnomalyRepository.class);
        when(anomalyRepository.save(any(Anomaly.class))).thenAnswer(call -> {
            anomalies.add(call.getArgument(0));
            return call.getArgument(0);
        });
        when(anomalyRepository.existsBySagaIdAndAnomalyType(anyString(), any()))
                .thenAnswer(call -> anomalies.stream()
                        .anyMatch(a -> a.getSagaId().equals(call.getArgument(0))
                                && a.getAnomalyType() == call.getArgument(1)));

        registry = new WorkflowRegistry();
        registry.load();
        parallelCheckout = registry.byType("PARALLEL_CHECKOUT").orElseThrow();
        parallelFulfilment = registry.byType("PARALLEL_FULFILMENT").orElseThrow();
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
    void oneArmObservedAfterTheOtherIsNotAnInvalidTransition() {
        assertThat(parallelCheckout.judgeTransition(
                "RUN_FRAUD_CHECK", "RESERVE_SEATS", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    @Test
    void theSameTwoArmsInReverseOrderAreAlsoNotInvalid() {
        assertThat(parallelCheckout.judgeTransition(
                "RESERVE_SEATS", "RUN_FRAUD_CHECK", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    @Test
    void theSpecificationReportsTheTwoArmsAsConcurrent() {
        assertThat(parallelCheckout.areConcurrent("RUN_FRAUD_CHECK", "RESERVE_SEATS")).isTrue();
        assertThat(parallelCheckout.areConcurrent("RESERVE_SEATS", "RUN_FRAUD_CHECK")).isTrue();
    }

    @Test
    void stepsOrderedAroundTheFanOutAreNotConcurrentWithItsArms() {
        assertThat(parallelCheckout.areConcurrent("OPEN_CART", "RESERVE_SEATS")).isFalse();
        assertThat(parallelCheckout.areConcurrent("RUN_FRAUD_CHECK", "SETTLE_ORDER")).isFalse();
        assertThat(parallelCheckout.areConcurrent("SETTLE_ORDER", "RESERVE_SEATS")).isFalse();
    }

    @Test
    void aDeclaredSequentialTransitionRemainsValid() {
        assertThat(parallelCheckout.judgeTransition(
                "OPEN_CART", "RUN_FRAUD_CHECK", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
        assertThat(orderSaga.judgeTransition(
                "CREATE_ORDER", "PROCESS_PAYMENT", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
    }

    @Test
    void anArmHandingOffToTheJoinRemainsValid() {
        assertThat(parallelCheckout.judgeTransition(
                "RESERVE_SEATS", "SETTLE_ORDER", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
    }

    @Test
    void movingBackwardsAcrossAJoinIsStillInvalid() {
        assertThat(parallelCheckout.judgeTransition(
                "SETTLE_ORDER", "RESERVE_SEATS", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.INVALID);
    }

    @Test
    void branchesOfADecisionAreNotConcurrentAndStayInvalid() {
        assertThat(loanApproval.areConcurrent("DISBURSE_FUNDS", "HUMAN_REVIEW")).isFalse();
        assertThat(loanApproval.judgeTransition("ASSESS_RISK", "HUMAN_REVIEW", "APPROVED"))
                .isEqualTo(TransitionVerdict.INVALID);
    }

    @Test
    void aWorkflowWithoutParallelismIsUnaffected() {
        assertThat(orderSaga.areConcurrent("PROCESS_PAYMENT", "RESERVE_INVENTORY")).isFalse();
        assertThat(orderSaga.judgeTransition(
                "PROCESS_PAYMENT", "CREATE_ORDER", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.INVALID);
        assertThat(orderSaga.judgeTransition(
                "RESERVE_INVENTORY", "CREATE_ORDER", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.INVALID);
    }

    @Test
    void aMissingIntermediateObservationIsStillUnknownAndStillNamed() {
        WorkflowSpec.TransitionJudgment judgment = orderSaga.explainTransition(
                "CREATE_ORDER", "RESERVE_INVENTORY", WorkflowSpec.SUCCESS_OUTCOME);

        assertThat(judgment.verdict()).isEqualTo(TransitionVerdict.UNKNOWN);
        assertThat(judgment.stepsInBetween()).containsExactly("PROCESS_PAYMENT");
    }

    @Test
    void aHealthyParallelRunRaisesNoInvalidTransition() {
        observe("e1", "CART_OPENED", 0);
        observe("e2", "SEATS_RESERVED", 1);
        observe("e3", "FRAUD_CHECK_PASSED", 2);
        observe("e4", "ORDER_SETTLED", 3);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void theSameRunWithItsArmsSwappedAlsoRaisesNothing() {
        observe("e1", "CART_OPENED", 0);
        observe("e2", "FRAUD_CHECK_PASSED", 1);
        observe("e3", "SEATS_RESERVED", 2);
        observe("e4", "ORDER_SETTLED", 3);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void aRunThatGoesBackwardsThroughAParallelWorkflowIsStillReported() {
        observe("e1", "CART_OPENED", 0);
        observe("e2", "FRAUD_CHECK_PASSED", 1);
        observe("e3", "SEATS_RESERVED", 2);

        observe("e4", "CART_OPENED", 3);

        assertThat(raised()).contains(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void anArmReportingAfterTheJoinIsAPostTerminalObservation() {
        observe("e1", "CART_OPENED", 0);
        observe("e2", "FRAUD_CHECK_PASSED", 1);
        observe("e3", "SEATS_RESERVED", 2);
        observe("e4", "ORDER_SETTLED", 3);
        observe("e5", "SEATS_RESERVED", 4);

        assertThat(raised()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void aCompensatingParallelRunRaisesNoInvalidTransition() {
        observeCompensatingFulfilment();

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    }

    @Test
    void aCompensatingParallelRunPromotesTheCompensationFailureNotATransitionFinding() {
        observeCompensatingFulfilment();

        List<Anomaly> standing = new ArrayList<>(anomalies);
        standing.add(anomaly(AnomalyType.MISSING_COMPENSATION_ACK));

        RcaResult result = analyse(standing);

        assertThat(result.getRootCause()).isEqualTo("COMPENSATION_STEP_FAILURE");
        assertThat(result.getRuleId()).isEqualTo("R10");
        assertThat(result.getCandidates())
                .noneSatisfy(c -> assertThat(c.getRuleId()).isEqualTo("R12"));
    }

    @Test
    void agenuineTransitionFindingIsStillDiagnosedAsSuch() {
        RcaResult result = analyse(List.of(anomaly(AnomalyType.INVALID_TRANSITION)));

        assertThat(result.getRootCause()).isEqualTo("INVALID_TRANSITION");
        assertThat(result.getRuleId()).isEqualTo("R12");
    }

    private void observeCompensatingFulfilment() {
        observe("f1", "BASKET_ACCEPTED", 0);
        observe("f2", "FUNDS_HELD", 1);
        observe("f3", "INVENTORY_REFUSED", 2);
        observe("f4", "FUNDS_RELEASE_REQUESTED", 3);
    }

    private void observe(String eventId, String eventType, long secondsIn) {
        correlationService.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SOURCE, RUN, eventId, null,
                        CorrelationBasis.DECLARED_KEY),
                null, eventType, "UNKNOWN", "a-service",
                T0.plusSeconds(secondsIn), "{}", Instant.now(),
                IngestedEvent.Causality.NONE));
    }

    private List<AnomalyType> raised() {
        return anomalies.stream().map(Anomaly::getAnomalyType).toList();
    }

    private SagaInstance run() {
        return instances.get(RUN.canonical());
    }

    private Anomaly anomaly(AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId((long) (type.ordinal() + 1));
        anomaly.setAnomalyType(type);
        anomaly.setSagaId(RUN.canonical());
        anomaly.setDetectedAt(T0);
        return anomaly;
    }

    private RcaResult analyse(List<Anomaly> standing) {
        SagaInstance saga = run() != null ? run() : new SagaInstance();
        saga.setSagaId(RUN.canonical());
        if (saga.getStartTime() == null) saga.setStartTime(T0);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));

        SagaEventRepository sagaEvents = mock(SagaEventRepository.class);
        when(sagaEvents.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenReturn(List.copyOf(events));

        AnomalyRepository anomalyRepository = mock(AnomalyRepository.class);
        when(anomalyRepository.findStandingBySagaId(anyString())).thenReturn(standing);

        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        RuleRegistry rules = new RuleRegistry();
        rules.load();

        return new RcaService(sagaInstances, sagaEvents, anomalyRepository, results, registry,
                new ObjectMapper(), new RcaSelector(), rules, new DetectionProperties(),
                mock(AlertSink.class), new ExecutionProjectionService(),
                EMPTY_FAILURE_SIGNALS, null)
                .analyze(RUN.canonical());
    }
}
