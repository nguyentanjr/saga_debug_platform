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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RegressionEvidenceCompletenessTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    private static final List<Observation> COMPENSATING_PARALLEL_RUN = List.of(
            new Observation("e1", "BASKET_ACCEPTED", 0),
            new Observation("e2", "INVENTORY_REFUSED", 1),
            new Observation("e3", "FUNDS_HELD", 2),
            new Observation("e4", "FUNDS_RELEASE_REQUESTED", 2));

    private static final List<Observation> OVERWRITTEN_ENDING = List.of(
            new Observation("f1", "ORDER_CREATED", 0),
            new Observation("f2", "INVENTORY_RESERVED", 1),
            new Observation("f3", "PAYMENT_FAILED", 2));

    @Test
    void anArmAnsweringAfterTheOtherArmFailedIsNotARegression() {
        observeAll(COMPENSATING_PARALLEL_RUN.subList(0, 3));

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    @Test
    void norIsItOnceTheWholeRunHasArrived() {
        observeAll(COMPENSATING_PARALLEL_RUN);

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    @Test
    void theEventThatUsedToTriggerItFollowedByTheOneThatExplainsIt() {
        observe(COMPENSATING_PARALLEL_RUN.get(0));
        observe(COMPENSATING_PARALLEL_RUN.get(1));
        observe(COMPENSATING_PARALLEL_RUN.get(2));
        observe(COMPENSATING_PARALLEL_RUN.get(3));

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
        assertThat(run().getStatus())
                .as("the run is compensating, which is what the compensation rules diagnose")
                .isEqualTo(SagaStatus.COMPENSATING);
    }

    @Test
    void noDeliveryOrderOfThatRunProducesTheFinding() {
        for (List<Observation> order : permutations(COMPENSATING_PARALLEL_RUN)) {
            setUp();
            observeAll(order);

            verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
            assertThat(run().getStatus())
                    .as("delivery order %s", ids(order))
                    .isEqualTo(SagaStatus.COMPENSATING);
        }
    }

    @Test
    void aRunThatContradictedItsOwnEndingIsStillReported() {
        observeAll(OVERWRITTEN_ENDING);

        ArgumentCaptor<EvidenceTimeline.Regression> found =
                ArgumentCaptor.forClass(EvidenceTimeline.Regression.class);
        verify(anomalyDetectionService, atLeastOnce())
                .detectStateRegression(eq(RUN.canonical()), found.capture());
        assertThat(found.getValue().from()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(found.getValue().to()).isEqualTo(SagaStatus.FAILED);
        assertThat(found.getValue().from().isTerminal())
                .as("the contradicted status is always an ending")
                .isTrue();
    }

    @Test
    void everyDeliveryOrderOfThatRunProducesTheFinding() {
        for (List<Observation> order : permutations(OVERWRITTEN_ENDING)) {
            setUp();
            observeAll(order);

            verify(anomalyDetectionService, atLeastOnce())
                    .detectStateRegression(anyString(), any());
        }
    }

    @Test
    void anEndingReachedByCompensationIsProtectedToo() {
        observeAll(List.of(
                new Observation("g1", "ORDER_CREATED", 0),
                new Observation("g2", "PAYMENT_COMPLETED", 1),
                new Observation("g3", "INVENTORY_FAILED", 2),
                new Observation("g4", "REFUND_PAYMENT", 3),
                new Observation("g5", "PAYMENT_REFUNDED", 4),
                new Observation("g6", "PAYMENT_FAILED", 5)));

        ArgumentCaptor<EvidenceTimeline.Regression> found =
                ArgumentCaptor.forClass(EvidenceTimeline.Regression.class);
        verify(anomalyDetectionService, atLeastOnce())
                .detectStateRegression(eq(RUN.canonical()), found.capture());
        assertThat(found.getValue().from()).isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    void healthyRunsRaiseNothingInEitherDirection() {
        List<Observation> happy = List.of(
                new Observation("h1", "ORDER_CREATED", 0),
                new Observation("h2", "PAYMENT_COMPLETED", 1),
                new Observation("h3", "INVENTORY_RESERVED", 2));

        observeAll(happy);
        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());

        setUp();
        observeAll(happy.reversed());
        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void aHealthyParallelRunRaisesNothingInEitherArmOrder() {
        observeAll(List.of(
                new Observation("p1", "BASKET_ACCEPTED", 0),
                new Observation("p2", "FUNDS_HELD", 1),
                new Observation("p3", "INVENTORY_HELD", 2),
                new Observation("p4", "BASKET_CONFIRMED", 3)));
        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());

        setUp();
        observeAll(List.of(
                new Observation("q1", "BASKET_ACCEPTED", 0),
                new Observation("q2", "INVENTORY_HELD", 1),
                new Observation("q3", "FUNDS_HELD", 2),
                new Observation("q4", "BASKET_CONFIRMED", 3)));
        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    @Test
    void aContestedPairStillEvidencesNothing() {
        observeAll(List.of(
                new Observation("c1", "BASKET_ACCEPTED", 0),
                new Observation("c2", "FUNDS_REFUSED", 1),
                new Observation("c3", "INVENTORY_HELD", 1)));

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

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

    private SagaInstance run() {
        return instances.get(RUN.canonical());
    }

    private void observeAll(List<Observation> observations) {
        observations.forEach(this::observe);
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

    private static List<String> ids(List<Observation> observations) {
        return observations.stream().map(Observation::eventId).toList();
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
}
