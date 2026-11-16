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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObserverReplayTest {
    private static final FailureSignalRegistry EMPTY_FAILURE_SIGNALS =
            new FailureSignalRegistry("");

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final ExecutionKey RUN = ExecutionKey.of("shop", "ORD-1");

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
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(call -> {
            SagaInstance instance = call.getArgument(0);
            instances.put(instance.getSagaId(), instance);
            return instance;
        });

        SagaEventRepository sagaEventRepository = mock(SagaEventRepository.class);
        when(sagaEventRepository.findBySourceNameAndEventId(anyString(), anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getSourceName().equals(call.getArgument(0)))
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
    void aReReadOfTheSameRecordIsNotReportedAgainstTheSource() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));

        verify(anomalyDetectionService, never())
                .detectDuplicateEvent(anyString(), anyString(), anyString(), any(), any());
        verify(anomalyDetectionService, never())
                .detectDuplicateEvent(anyString(), anyString(), anyString());
    }

    @Test
    void aGenuineSourceDuplicateIsStillReported() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-42"));

        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e1", "ORDER_CREATED", "orders-0-17", "orders-0-42");
    }

    @Test
    void aReplayDoesNotStoreTheEventTwice() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e2", "PAYMENT_COMPLETED", "payments-0-3"));

        List<SagaEvent> afterFirstPass = List.copyOf(events);

        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e2", "PAYMENT_COMPLETED", "payments-0-3"));

        assertThat(events).hasSize(2).containsExactlyElementsOf(afterFirstPass);
    }

    @Test
    void theRecordThatCarriedAnEventIsStoredWithIt() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));

        assertThat(events).singleElement()
                .extracting(SagaEvent::getTransportRef).isEqualTo("orders-0-17");
    }

    @Test
    void anObservationWithNoTransportIdentityKeepsItsPreviousMeaning() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", null));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", null));

        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e1", "ORDER_CREATED", null, null);
    }

    @Test
    void aStoredEventWithNoRecordIsNotAssumedToBeTheSameRecord() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", null));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));

        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e1", "ORDER_CREATED", null, "orders-0-17");
    }

    @Test
    void theCacheStillSkipsARedeliveryOfTheSameRecord() {
        DeduplicationCache cache = new DeduplicationCache();
        cache.markProcessed("shop::e1", "orders-0-17");

        assertThat(cache.seen("shop::e1")).isTrue();
        assertThat(cache.seenOnDifferentRecord("shop::e1", "orders-0-17")).isFalse();
    }

    @Test
    void theCacheNoLongerSwallowsASecondRecordCarryingTheSameEvent() {
        DeduplicationCache cache = new DeduplicationCache();
        cache.markProcessed("shop::e1", "orders-0-17");

        assertThat(cache.seenOnDifferentRecord("shop::e1", "orders-0-42")).isTrue();
    }

    @Test
    void theCacheDoesNotClaimADifferenceItCannotSee() {
        DeduplicationCache cache = new DeduplicationCache();
        cache.markProcessed("shop::e1");

        assertThat(cache.seen("shop::e1")).isTrue();
        assertThat(cache.seenOnDifferentRecord("shop::e1", "orders-0-42")).isFalse();
        assertThat(cache.seenOnDifferentRecord("shop::e1", null)).isFalse();
    }

    @Test
    void aReplayLeavesRcaWithNoDuplicateCauseToPromote() {
        RcaResult result = analyseWith(standingAnomalies());

        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRuleId()).isNull();
        assertThat(result.getCandidates())
                .noneSatisfy(candidate -> assertThat(candidate.getRuleId()).isEqualTo("R13"));
    }

    @Test
    void aGenuineDuplicateStillReachesRcaAndIsPromotable() {
        RcaResult result = analyseWith(standingAnomalies(AnomalyType.EXACT_DUPLICATE_EVENT));

        assertThat(result.getRootCause()).isEqualTo("DUPLICATE_DELIVERY");
        assertThat(result.getRuleId()).isEqualTo("R13");
    }

    @Test
    void aWithdrawnDuplicateFindingStaysOutOfTheDiagnosis() {
        Anomaly withdrawn = anomaly(AnomalyType.EXACT_DUPLICATE_EVENT);
        withdrawn.setRetractedAt(T0.plusSeconds(60));
        withdrawn.setRetractionReason("the records turned out to be one record read twice");

        RcaResult result = analyseWith(List.of());

        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRuleId()).isNull();
    }

    private IngestedEvent observation(String eventId, String eventType, String transportRef) {
        return new IngestedEvent(
                new IngestedEvent.Identity("shop", RUN, eventId, null,
                        CorrelationBasis.DECLARED_KEY),
                null, eventType, "UNKNOWN", "unknown", T0, "{}", T0,
                IngestedEvent.Causality.NONE, transportRef);
    }

    private List<Anomaly> standingAnomalies(AnomalyType... types) {
        List<Anomaly> standing = new ArrayList<>();
        for (AnomalyType type : types) {
            standing.add(anomaly(type));
        }
        return standing;
    }

    private Anomaly anomaly(AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId((long) (type.ordinal() + 1));
        anomaly.setAnomalyType(type);
        anomaly.setSagaId(RUN.canonical());
        anomaly.setDetectedAt(T0);
        return anomaly;
    }

    private RcaResult analyseWith(List<Anomaly> standing) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId(RUN.canonical());
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setStartTime(T0);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));

        SagaEventRepository sagaEvents = mock(SagaEventRepository.class);
        when(sagaEvents.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(List.of());

        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.findStandingBySagaId(eq(RUN.canonical()))).thenReturn(standing);

        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        RuleRegistry rules = new RuleRegistry();
        rules.load();
        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();

        return new RcaService(sagaInstances, sagaEvents, anomalies, results, workflows,
                new ObjectMapper(), new RcaSelector(), rules, new DetectionProperties(),
                mock(AlertSink.class), new ExecutionProjectionService(),
                EMPTY_FAILURE_SIGNALS, null)
                .analyze(RUN.canonical());
    }
}
