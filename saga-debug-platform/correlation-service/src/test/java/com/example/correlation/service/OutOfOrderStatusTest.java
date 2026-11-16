package com.example.correlation.service;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.EvidenceTimeline;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutOfOrderStatusTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    private static final List<Observation> HAPPY_PATH = List.of(
            new Observation("e1", "ORDER_CREATED", 0),
            new Observation("e2", "PAYMENT_COMPLETED", 1),
            new Observation("e3", "INVENTORY_RESERVED", 2));

    private static final List<Observation> COMPENSATED_PATH = List.of(
            new Observation("e1", "ORDER_CREATED", 0),
            new Observation("e2", "PAYMENT_COMPLETED", 1),
            new Observation("e3", "INVENTORY_FAILED", 2),
            new Observation("e4", "REFUND_PAYMENT", 3),
            new Observation("e5", "PAYMENT_REFUNDED", 4));

    private record Observation(String eventId, String eventType, long secondsIn) {
        Instant at() {
            return T0.plusSeconds(secondsIn);
        }
    }

    private Map<String, SagaInstance> instances;
    private List<SagaEvent> events;
    private AnomalyDetectionService anomalyDetectionService;
    private CorrelationService correlationService;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        events = new ArrayList<>();

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

        anomalyDetectionService = mock(AnomalyDetectionService.class);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    @Test
    void aRunReadInOrderReadsAsWhatItWas() {
        observeAll(HAPPY_PATH);

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getStartTime()).isEqualTo(T0);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    @Test
    void aRunReadBackwardsReadsTheSameAsOneReadForwards() {
        observeAll(reversed(HAPPY_PATH));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getStartTime()).isEqualTo(T0);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    @Test
    void aCompletionReadBeforeTheStepBeforeItStillEndsTheRun() {
        observe(HAPPY_PATH.get(0));
        observe(HAPPY_PATH.get(2));
        observe(HAPPY_PATH.get(1));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    @Test
    void everyDeliveryOrderOfOneRunProducesTheSameReading() {
        List<String> readings = new ArrayList<>();
        for (List<Observation> order : permutations(COMPENSATED_PATH)) {
            setUp();
            observeAll(order);
            readings.add(run().getStatus() + " " + run().getStartTime() + " " + run().getEndTime());
        }

        assertThat(readings).hasSize(120)
                .containsOnly("COMPENSATED " + T0 + " " + T0.plusSeconds(4));
    }

    @Test
    void aRedeliveredEventChangesNothingAndIsReported() {
        observeAll(HAPPY_PATH);
        observe(HAPPY_PATH.get(1));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
        assertThat(events).hasSize(3);

        verify(anomalyDetectionService)
                .detectDuplicateEvent(RUN.canonical(), "e2", "PAYMENT_COMPLETED", null, null);
    }

    @Test
    void replayingAnEntireRunLeavesItExactlyAsItWas() {
        observeAll(HAPPY_PATH);
        SagaStatus afterFirstPass = run().getStatus();
        Instant endAfterFirstPass = run().getEndTime();

        observeAll(reversed(HAPPY_PATH));

        assertThat(run().getStatus()).isEqualTo(afterFirstPass);
        assertThat(run().getEndTime()).isEqualTo(endAfterFirstPass);
        assertThat(events).hasSize(3);
    }

    @Test
    void aLateArrivalIsNotTreatedAsTheNewestThingToHaveHappened() {
        observe(HAPPY_PATH.get(0));
        observe(HAPPY_PATH.get(2));
        Instant lastUpdatedBefore = run().getLastUpdated();

        observe(HAPPY_PATH.get(1));

        assertThat(run().getLastUpdated()).isEqualTo(lastUpdatedBefore);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void aLateStartEventMovesTheRunsBeginningEarlier() {
        observe(HAPPY_PATH.get(1));
        observe(HAPPY_PATH.get(2));
        assertThat(run().getStartTime()).isEqualTo(T0.plusSeconds(1));

        observe(HAPPY_PATH.get(0));

        assertThat(run().getStartTime()).isEqualTo(T0);
    }

    @Test
    void whenTheClockCannotSeparateEventsTheWorkflowOrdersThem() {
        observe(new Observation("e3", "INVENTORY_RESERVED", 0));
        observe(new Observation("e1", "ORDER_CREATED", 0));
        observe(new Observation("e2", "PAYMENT_COMPLETED", 0));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void anOutcomeTheEvidenceCannotDecideIsNotDecided() {
        observe(new Observation("e1", "ORDER_CREATED", 0));
        observe(new Observation("e2", "PAYMENT_COMPLETED", 1));
        observe(new Observation("e3", "INVENTORY_RESERVED", 2));
        observe(new Observation("e4", "PAYMENT_REFUNDED", 2));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.IN_PROGRESS);
        assertThat(run().getEndTime()).isNull();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<SagaEvent>> contested = ArgumentCaptor.forClass(List.class);
        verify(anomalyDetectionService)
                .detectIndeterminateOrder(eq(RUN.canonical()), contested.capture());
        assertThat(contested.getValue())
                .extracting(SagaEvent::getEventType)
                .containsExactlyInAnyOrder("INVENTORY_RESERVED", "PAYMENT_REFUNDED");
    }

    @Test
    void aDecidableRunRaisesNoIndeterminacyFinding() {
        observeAll(reversed(HAPPY_PATH));

        verify(anomalyDetectionService, never())
                .detectIndeterminateOrder(anyString(), any());
    }

    @Test
    void aCompensationReadBackwardsStillReachesCompensated() {
        observeAll(reversed(COMPENSATED_PATH));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(4));
    }

    @Test
    void aCompensationAcknowledgementReadEarlyStillEndsTheRun() {
        observe(COMPENSATED_PATH.get(0));
        observe(COMPENSATED_PATH.get(4));
        observe(COMPENSATED_PATH.get(2));
        observe(COMPENSATED_PATH.get(1));
        observe(COMPENSATED_PATH.get(3));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    void anEventThatHappenedAfterTheRunEndedIsReported() {
        observeAll(HAPPY_PATH);

        observe(new Observation("e9", "INVENTORY_FAILED", 9));

        verify(anomalyDetectionService).detectForbiddenPostTerminal(
                eq(RUN.canonical()),
                eq(SagaStatus.COMPLETED),
                org.mockito.ArgumentMatchers.argThat(
                        terminal -> "INVENTORY_RESERVED".equals(terminal.getEventType())),
                org.mockito.ArgumentMatchers.argThat(late -> "e9".equals(late.getEventId())));
    }

    @Test
    void anEventThatOnlyArrivedAfterTheEndIsNotReportedAsHappeningAfterIt() {
        observe(HAPPY_PATH.get(0));
        observe(HAPPY_PATH.get(2));

        observe(HAPPY_PATH.get(1));

        verify(anomalyDetectionService, never())
                .detectForbiddenPostTerminal(anyString(), any(), any(), any());
    }

    @Test
    void readingARunOutOfOrderIsNotAStateRegression() {
        observeAll(reversed(HAPPY_PATH));

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    @Test
    void norIsReadingACompensationOutOfOrder() {
        observeAll(reversed(COMPENSATED_PATH));

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    @Test
    void aRunThatReallyMovedBackwardsIsStillReported() {
        observe(new Observation("e1", "ORDER_CREATED", 0));
        observe(new Observation("e2", "INVENTORY_RESERVED", 1));
        observe(new Observation("e3", "PAYMENT_FAILED", 2));

        ArgumentCaptor<EvidenceTimeline.Regression> found =
                ArgumentCaptor.forClass(EvidenceTimeline.Regression.class);
        verify(anomalyDetectionService)
                .detectStateRegression(eq(RUN.canonical()), found.capture());
        assertThat(found.getValue().from()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(found.getValue().to()).isEqualTo(SagaStatus.FAILED);
        assertThat(found.getValue().later().getEventType()).isEqualTo("PAYMENT_FAILED");
    }

    @Test
    void aRealRegressionIsFoundWhicheverOrderItIsReadIn() {
        observe(new Observation("e3", "PAYMENT_FAILED", 2));
        observe(new Observation("e1", "ORDER_CREATED", 0));
        observe(new Observation("e2", "INVENTORY_RESERVED", 1));

        verify(anomalyDetectionService).detectStateRegression(anyString(), any());
    }

    @Test
    void arrivalOrderDisagreeingWithEventTimeIsStillItsOwnFinding() {
        observe(HAPPY_PATH.get(0));
        observe(HAPPY_PATH.get(2));
        observe(HAPPY_PATH.get(1));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<SagaEvent>> handed = ArgumentCaptor.forClass(List.class);
        verify(anomalyDetectionService, org.mockito.Mockito.atLeastOnce())
                .detectOutOfOrder(eq(RUN.canonical()), handed.capture());

        List<SagaEvent> lastCall = handed.getValue();
        assertThat(lastCall).hasSize(3);

        assertThat(lastCall).extracting(SagaEvent::getEventId)
                .containsExactly("e1", "e3", "e2");
    }

    private SagaInstance run() {
        return instances.get(RUN.canonical());
    }

    private void observeAll(List<Observation> observations) {
        observations.forEach(this::observe);
    }

    private static List<Observation> reversed(List<Observation> observations) {
        List<Observation> copy = new ArrayList<>(observations);
        java.util.Collections.reverse(copy);
        return copy;
    }

    private static List<List<Observation>> permutations(List<Observation> observations) {
        if (observations.isEmpty()) return List.of(List.of());
        List<List<Observation>> all = new ArrayList<>();
        for (int i = 0; i < observations.size(); i++) {
            List<Observation> rest = new ArrayList<>(observations);
            Observation head = rest.remove(i);
            for (List<Observation> tail : permutations(rest)) {
                List<Observation> one = new ArrayList<>();
                one.add(head);
                one.addAll(tail);
                all.add(one);
            }
        }
        return all;
    }

    @Test
    void aFindingTheClocksSupportedIsWithdrawnWhenTheCausalChainCloses() {
        Instant base = Instant.parse("2026-10-03T19:33:21Z");
        observeCaused("inventory", "INVENTORY_RESERVED", "payment", base.plusMillis(787));
        observeCaused("order", "ORDER_CREATED", null, base.plusMillis(1517));
        verify(anomalyDetectionService).detectForbiddenPostTerminal(eq(RUN.canonical()), any(), any(), any());

        observeCaused("payment", "PAYMENT_COMPLETED", "order", base.plusMillis(1559));

        org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(anomalyDetectionService);
        inOrder.verify(anomalyDetectionService).detectForbiddenPostTerminal(eq(RUN.canonical()), any(), any(), any());
        inOrder.verify(anomalyDetectionService).withdrawUnprovenOrder(RUN.canonical(),
                com.example.correlation.domain.AnomalyType.FORBIDDEN_POST_TERMINAL);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getEndTime()).isAfterOrEqualTo(run().getStartTime());
    }

    private void observeCaused(String eventId, String eventType, String causedBy, Instant at) {
        correlationService.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SOURCE, RUN, eventId, null, CorrelationBasis.DECLARED_KEY),
                null, eventType, "UNKNOWN", "a-service", at, "{}", Instant.now(),
                new IngestedEvent.Causality(causedBy, null, null, null)));
    }

    private void observe(Observation observation) {
        correlationService.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SOURCE, RUN, observation.eventId(), null,
                        CorrelationBasis.DECLARED_KEY),
                null,
                observation.eventType(),
                "UNKNOWN",
                "a-service",
                observation.at(),
                "{}",
                Instant.now(),
                IngestedEvent.Causality.NONE));
    }
}
