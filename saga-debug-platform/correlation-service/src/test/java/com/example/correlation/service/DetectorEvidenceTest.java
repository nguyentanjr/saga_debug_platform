package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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

class DetectorEvidenceTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    private Map<String, SagaInstance> instances;
    private List<SagaEvent> events;
    private List<Anomaly> anomalies;

    private SagaInstanceRepository sagaInstanceRepository;
    private SagaEventRepository sagaEventRepository;
    private AnomalyRepository anomalyRepository;
    private AnomalyDetectionService anomalyDetectionService;
    private CorrelationService correlationService;

    @BeforeEach
    @SuppressWarnings("unchecked")
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
        when(sagaInstanceRepository.findByStatusIn(any(), any())).thenAnswer(call -> {
            Collection<SagaStatus> wanted = call.getArgument(0);
            return instances.values().stream()
                    .filter(saga -> wanted.contains(saga.getStatus()))
                    .toList();
        });

        sagaEventRepository = mock(SagaEventRepository.class);
        when(sagaEventRepository.findBySourceNameAndEventId(anyString(), anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getEventId().equals(call.getArgument(1)))
                        .findFirst());
        when(sagaEventRepository.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenAnswer(call -> eventsOf(call.getArgument(0)));
        when(sagaEventRepository.findTopBySagaIdOrderByTimestampDesc(anyString()))
                .thenAnswer(call -> eventsOf(call.getArgument(0)).stream()
                        .reduce((first, second) -> second));
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
                .thenAnswer(call -> standing().stream()
                        .anyMatch(a -> a.getSagaId().equals(call.getArgument(0))
                                && a.getAnomalyType() == call.getArgument(1)));
        when(anomalyRepository.findBySagaId(anyString()))
                .thenAnswer(call -> anomalies.stream()
                        .filter(a -> a.getSagaId().equals(call.getArgument(0)))
                        .toList());
        when(anomalyRepository.retractClaims(anyString(), any(), any(), anyString()))
                .thenAnswer(call -> {
                    Collection<AnomalyType> types = call.getArgument(1);
                    int withdrawn = 0;
                    for (Anomaly a : anomalies) {
                        if (a.getSagaId().equals(call.getArgument(0))
                                && types.contains(a.getAnomalyType())
                                && a.getRetractedAt() == null) {
                            a.setRetractedAt(call.getArgument(2));
                            a.setRetractionReason(call.getArgument(3));
                            withdrawn++;
                        }
                    }
                    return withdrawn;
                });

        this.anomalyRepository = anomalyRepository;
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        wire(registry);
    }

    private void wire(WorkflowRegistry registry) {
        anomalyDetectionService = new AnomalyDetectionService(
                sagaInstanceRepository, sagaEventRepository, anomalyRepository,
                registry, new ObjectMapper(), new DetectionProperties(),
                new ExecutionProjectionService());

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    @Test
    void anOrphanFindingIsWithdrawnWhenTheStartEventArrives() {
        observe("e2", "PAYMENT_COMPLETED", 1);
        assertThat(standingTypes()).contains(AnomalyType.ORPHAN_EVENT);

        observe("e1", "ORDER_CREATED", 0);

        assertThat(standingTypes()).doesNotContain(AnomalyType.ORPHAN_EVENT);
        assertThat(anomalies).anyMatch(a -> a.getAnomalyType() == AnomalyType.ORPHAN_EVENT
                && a.getRetractedAt() != null);
    }

    @Test
    void anOrphanFindingIsWithdrawnEvenWhenTheWorkflowWasNeverInDoubt() {
        submit(RUN, "e2", "PAYMENT_COMPLETED", T0.plusSeconds(1), Instant.now(), null, "ORDER_SAGA");
        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
        assertThat(standingTypes()).contains(AnomalyType.ORPHAN_EVENT);

        submit(RUN, "e1", "ORDER_CREATED", T0, Instant.now(), null, "ORDER_SAGA");

        assertThat(standingTypes()).doesNotContain(AnomalyType.ORPHAN_EVENT);
    }

    @Test
    void aRunReadFromItsStartRaisesNoOrphanFinding() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e2", "PAYMENT_COMPLETED", 1);

        assertThat(standingTypes()).doesNotContain(AnomalyType.ORPHAN_EVENT);
    }

    @Test
    void anOrphanFindingNamesTheWorkflowItIsAbout() {
        ExecutionKey fulfilment = ExecutionKey.of(SOURCE, "run-f");
        submit(fulfilment, "f2", "CHARGE_COMPLETED", T0.plusSeconds(1), Instant.now(),
                null, "FULFILLMENT_SAGA");

        Anomaly orphan = anomalies.stream()
                .filter(a -> a.getAnomalyType() == AnomalyType.ORPHAN_EVENT)
                .findFirst().orElseThrow();

        assertThat(orphan.getDescription()).doesNotContain("ORDER_CREATED");
        assertThat(orphan.getThresholdSource()).doesNotContain("ORDER_SAGA");
        assertThat(orphan.getExpectedValue()).contains("SHIPMENT_CREATED");
    }

    @Test
    void anUntracedEventInATracedRunIsFoundInEitherDeliveryOrder() {
        observeTraced("e1", "ORDER_CREATED", 0, null);
        observeTraced("e2", "PAYMENT_COMPLETED", 1, "trace-abc");

        assertThat(standingTypes()).contains(AnomalyType.MISSING_TRACE_CONTEXT);
    }

    @Test
    void theSameRunReadTheOtherWayRoundReportsTheSameThing() {
        observeTraced("e2", "PAYMENT_COMPLETED", 1, "trace-abc");
        observeTraced("e1", "ORDER_CREATED", 0, null);

        assertThat(standingTypes()).contains(AnomalyType.MISSING_TRACE_CONTEXT);
    }

    @Test
    void aRunThatNeverReportsATraceIsNotAFinding() {
        observeTraced("e1", "ORDER_CREATED", 0, null);
        observeTraced("e2", "PAYMENT_COMPLETED", 1, null);

        assertThat(standingTypes()).doesNotContain(AnomalyType.MISSING_TRACE_CONTEXT);
    }

    @Test
    void anEventJustReceivedDoesNotMakeItsRunLookStuck() {
        Instant now = Instant.now();
        observeAt("e1", "ORDER_CREATED", now.minus(Duration.ofMinutes(10)), now);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standingTypes()).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    @Test
    void aRunNothingHasBeenHeardFromIsStillReportedAsStuck() {
        Instant longAgo = Instant.now().minus(Duration.ofMinutes(10));
        observeAt("e1", "ORDER_CREATED", longAgo, longAgo);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standingTypes()).contains(AnomalyType.SAGA_STUCK);
    }

    @Test
    void aSourceStampingTheFutureDoesNotHideARunThatWentQuiet() {
        Instant longAgo = Instant.now().minus(Duration.ofMinutes(10));
        observeAt("e1", "ORDER_CREATED", Instant.now().plus(Duration.ofHours(1)), longAgo);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standingTypes()).contains(AnomalyType.SAGA_STUCK);
    }

    private List<SagaEvent> eventsOf(String sagaId) {
        return events.stream()
                .filter(e -> e.getSagaId().equals(sagaId))
                .sorted(Comparator.comparing(SagaEvent::getTimestamp))
                .toList();
    }

    @Test
    void aWorkflowWithoutAStepMapIsHeldToItsDeclaredStepTimeout() {
        wire(declaredRegistry());
        Instant heard = Instant.now().minusSeconds(45);
        observeAt("e1", "BOOK_ROOM_REQUESTED", heard.minusSeconds(2), heard.minusSeconds(2));
        observeAt("e2", "ROOM_BOOKED", heard.minusSeconds(1), heard.minusSeconds(1));
        observeAt("e3", "TAKE_PAYMENT_REQUESTED", heard, heard);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standing()).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK)
                .singleElement()
                .satisfies(a -> assertThat(a.getThresholdSource()).isEqualTo("workflow:TAKE_PAYMENT.timeout"));
    }

    @Test
    void aWorkflowWithoutAStepMapHasItsTransitionsJudged() {
        wire(declaredRegistry());
        observe("e1", "BOOK_ROOM_REQUESTED", 0);
        observe("e2", "ROOM_BOOKED", 1);
        observe("e3", "TAKE_PAYMENT_REQUESTED", 2);
        observe("e4", "PAYMENT_ACCEPTED", 3);
        observe("e5", "ROOM_REJECTED", 4);

        assertThat(standingTypes()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void paymentRequestedAfterARejectedRoomIsAnInvalidTransition() {
        wire(declaredRegistry());
        observe("e1", "BOOK_ROOM_REQUESTED", 0);
        observe("e2", "ROOM_REJECTED", 1);
        observe("e3", "TAKE_PAYMENT_REQUESTED", 2);

        assertThat(standingTypes()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void theRealPathsOfThatWorkflowRaiseNoTransitionFinding() {
        wire(declaredRegistry());
        observe("e1", "BOOK_ROOM_REQUESTED", 0);
        observe("e2", "ROOM_BOOKED", 1);
        observe("e3", "TAKE_PAYMENT_REQUESTED", 2);
        observe("e4", "PAYMENT_ACCEPTED", 3);
        ExecutionKey compensated = ExecutionKey.of(SOURCE, "run-2");
        observeIn(compensated, "c1", "BOOK_ROOM_REQUESTED", 0);
        observeIn(compensated, "c2", "ROOM_BOOKED", 1);
        observeIn(compensated, "c3", "TAKE_PAYMENT_REQUESTED", 2);
        observeIn(compensated, "c4", "PAYMENT_REJECTED", 3);
        observeIn(compensated, "c5", "RELEASE_ROOM_REQUESTED", 4);
        observeIn(compensated, "c6", "ROOM_CANCELLED", 5);

        assertThat(standingTypes()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(instances.get(compensated.canonical()).getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    void silenceAfterAStartEventTheMapOmitsIsHeldToThatStepsTimeout() {
        Instant heard = Instant.now().minusSeconds(30);
        observeAt("e1", "ORDER_CREATED", heard.minusSeconds(1), heard.minusSeconds(1));
        observeAt("e2", "PAYMENT_STARTED", heard, heard);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standing()).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK)
                .singleElement()
                .satisfies(a -> assertThat(a.getThresholdSource()).isEqualTo("workflow:PROCESS_PAYMENT.timeout"));
    }

    @Test
    void aTransitionIntoAnEventTheMapOmitsIsJudged() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e2", "PAYMENT_FAILED", 1);
        observe("e3", "INVENTORY_STARTED", 2);

        assertThat(standingTypes()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    private static WorkflowRegistry declaredRegistry() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        WorkflowRegistry registry = new WorkflowRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", root.resolve("declarations").toString());
        registry.load();
        return registry;
    }

    private List<Anomaly> standing() {
        return anomalies.stream().filter(a -> a.getRetractedAt() == null).toList();
    }

    private List<AnomalyType> standingTypes() {
        return standing().stream().map(Anomaly::getAnomalyType).toList();
    }

    private void observe(String eventId, String eventType, long secondsIn) {
        observeIn(RUN, eventId, eventType, secondsIn);
    }

    private void observeIn(ExecutionKey run, String eventId, String eventType, long secondsIn) {
        submit(run, eventId, eventType, T0.plusSeconds(secondsIn), Instant.now(), null);
    }

    private void observeTraced(String eventId, String eventType, long secondsIn, String traceId) {
        submit(RUN, eventId, eventType, T0.plusSeconds(secondsIn), Instant.now(), traceId);
    }

    private void observeAt(String eventId, String eventType, Instant eventTime, Instant receivedAt) {
        submit(RUN, eventId, eventType, eventTime, receivedAt, null);
    }

    private SagaInstance run() {
        return instances.get(RUN.canonical());
    }

    private void submit(ExecutionKey run, String eventId, String eventType,
            Instant eventTime, Instant receivedAt, String traceId) {
        submit(run, eventId, eventType, eventTime, receivedAt, traceId, null);
    }

    private void submit(ExecutionKey run, String eventId, String eventType,
            Instant eventTime, Instant receivedAt, String traceId, String declaredWorkflow) {
        correlationService.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SOURCE, run, eventId, null,
                        CorrelationBasis.DECLARED_KEY),
                declaredWorkflow,
                eventType,
                "UNKNOWN",
                "a-service",
                eventTime,
                "{}",
                receivedAt,
                new IngestedEvent.Causality(null, null, null, traceId)));
    }
}
