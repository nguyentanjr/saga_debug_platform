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

/**
 * What the detectors are entitled to claim, given what the platform has actually seen.
 *
 * <p>Written from an audit of every detector against four questions this platform keeps
 * getting wrong in the same way: is it confusing the order it <em>read</em> events with the
 * order they <em>happened</em>; is it reading a gap in its own evidence as evidence of
 * absence; is it reporting its own limitation as the observed system's failure; and is it
 * presenting an inference as an observed fact.
 *
 * <p>Each test below names the detector it is about and the confusion it pins down.
 */
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

    // ── ORPHAN_EVENT: a start event not yet read is not a start event that never happened

    /**
     * The run's start event arrives after the rest. While it had not, "no event that starts
     * this workflow was seen" was true and worth saying; once it arrives the claim is
     * false, and an absence claim that later evidence falsifies has to be withdrawn.
     *
     * <p>Withdrawal used to happen only as a side effect of the workflow being resolved
     * late. That coupled a statement about one kind of evidence to a change in another:
     * where the workflow was already known — a source mapping that declares it, or a run
     * created by a start event of a different branch — the start event could arrive and the
     * finding would stand permanently, saying the observed system had emitted events for a
     * run that never began.
     */
    @Test
    void anOrphanFindingIsWithdrawnWhenTheStartEventArrives() {
        observe("e2", "PAYMENT_COMPLETED", 1);
        assertThat(standingTypes()).contains(AnomalyType.ORPHAN_EVENT);

        observe("e1", "ORDER_CREATED", 0);

        assertThat(standingTypes()).doesNotContain(AnomalyType.ORPHAN_EVENT);
        assertThat(anomalies).anyMatch(a -> a.getAnomalyType() == AnomalyType.ORPHAN_EVENT
                && a.getRetractedAt() != null);
    }

    /**
     * The same, for a run whose workflow was never in doubt.
     *
     * <p>A source mapping may name the workflow its events belong to, so a run can be
     * created by a mid-run event and still resolve immediately. Withdrawal hung off
     * resolution, and resolution had already happened, so nothing ever withdrew the claim:
     * the start event arrived, was stored, and the finding went on saying it had not.
     *
     * <p>Two different kinds of evidence — which workflow this is, and whether its start
     * was seen — were being answered by one event.
     */
    @Test
    void anOrphanFindingIsWithdrawnEvenWhenTheWorkflowWasNeverInDoubt() {
        submit(RUN, "e2", "PAYMENT_COMPLETED", T0.plusSeconds(1), Instant.now(), null, "ORDER_SAGA");
        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
        assertThat(standingTypes()).contains(AnomalyType.ORPHAN_EVENT);

        submit(RUN, "e1", "ORDER_CREATED", T0, Instant.now(), null, "ORDER_SAGA");

        assertThat(standingTypes()).doesNotContain(AnomalyType.ORPHAN_EVENT);
    }

    /** A run read in order never raises it at all. */
    @Test
    void aRunReadFromItsStartRaisesNoOrphanFinding() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e2", "PAYMENT_COMPLETED", 1);

        assertThat(standingTypes()).doesNotContain(AnomalyType.ORPHAN_EVENT);
    }

    /**
     * And the finding describes the workflow the run belongs to, not the one this platform
     * was first written for. The text named ORDER_CREATED and cited
     * {@code workflow:ORDER_SAGA.start_event} for every workflow, so a fulfillment saga was
     * told it should have started with an event no fulfillment saga has ever emitted — an
     * assumption of the platform's presented as the specification's requirement.
     */
    @Test
    void anOrphanFindingNamesTheWorkflowItIsAbout() {
        // Declared by the source's mapping, so the run is known to be a fulfillment saga
        // even though the event that would have said so has not been read yet. That is
        // exactly when the finding has to name the right workflow.
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

    // ── MISSING_TRACE_CONTEXT: a finding must not depend on which event was read first

    /**
     * One event of a traced run carries no trace. That is true of the evidence however the
     * events arrive, so the finding has to be too.
     *
     * <p>It was not: the detector was handed only the event that had just arrived and asked
     * whether any sibling <em>already stored</em> carried a trace. Read the untraced event
     * first and no sibling had one yet, so it went unreported and was never looked at
     * again. The observed system's broken propagation was invisible whenever delivery
     * happened to put the untraced event first.
     */
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

    /** A run no part of which reports a trace says something about the source, not a fault. */
    @Test
    void aRunThatNeverReportsATraceIsNotAFinding() {
        observeTraced("e1", "ORDER_CREATED", 0, null);
        observeTraced("e2", "PAYMENT_COMPLETED", 1, null);

        assertThat(standingTypes()).doesNotContain(AnomalyType.MISSING_TRACE_CONTEXT);
    }

    // ── SAGA_STUCK: silence is measured on the clock that can hear ────────────

    /**
     * An event the platform received a moment ago, stamped by its source ten minutes in the
     * past — a backfill, a replay, or simply a source whose clock runs behind.
     *
     * <p>"Stuck" is a claim that nothing has happened for a while. The detector measured
     * that as {@code now - event.timestamp}: the observer's clock minus the source's. Any
     * lag between the two became elapsed silence, so a run whose event had just been read
     * was reported as having gone quiet before it was even ingested. Reproduced live: an
     * ORDER_CREATED stamped ten minutes earlier and injected produced
     * {@code SAGA_STUCK: no event since ...} within one scheduler pass.
     *
     * <p>Whether the platform has heard anything is a question only its own clock can
     * answer, and {@code receivedAt} is where that answer is recorded.
     */
    @Test
    void anEventJustReceivedDoesNotMakeItsRunLookStuck() {
        Instant now = Instant.now();
        observeAt("e1", "ORDER_CREATED", now.minus(Duration.ofMinutes(10)), now);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standingTypes()).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    /** A run the platform genuinely has not heard from is still reported. */
    @Test
    void aRunNothingHasBeenHeardFromIsStillReportedAsStuck() {
        Instant longAgo = Instant.now().minus(Duration.ofMinutes(10));
        observeAt("e1", "ORDER_CREATED", longAgo, longAgo);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standingTypes()).contains(AnomalyType.SAGA_STUCK);
    }

    /**
     * The reverse skew: a source stamping events in the future. The platform has heard
     * nothing for ten minutes, which is the fact that matters.
     */
    @Test
    void aSourceStampingTheFutureDoesNotHideARunThatWentQuiet() {
        Instant longAgo = Instant.now().minus(Duration.ofMinutes(10));
        observeAt("e1", "ORDER_CREATED", Instant.now().plus(Duration.ofHours(1)), longAgo);

        anomalyDetectionService.detectStuckSagas();

        assertThat(standingTypes()).contains(AnomalyType.SAGA_STUCK);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private List<SagaEvent> eventsOf(String sagaId) {
        return events.stream()
                .filter(e -> e.getSagaId().equals(sagaId))
                .sorted(Comparator.comparing(SagaEvent::getTimestamp))
                .toList();
    }

    // ── L16: a workflow that never wrote validEventTypes ──────────────────────

    /**
     * ROOM_RESERVATION as the observer runs it, from {@code declarations/}. It has no
     * validEventTypes map, and every step lookup used to come back empty: the live hotel run
     * that went quiet after TAKE_PAYMENT_REQUESTED was reported stuck at the 60s default,
     * not at TAKE_PAYMENT's declared 30s. Forty-five seconds separates the two.
     */
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

    /**
     * And its transitions are judged: a room outcome after payment completed the run.
     * (Payment requested after a room rejection is the more natural case; until L22 a FAILED
     * outcome was accepted towards any declared target, so it is pinned separately below.)
     */
    @Test
    void aWorkflowWithoutAStepMapHasItsTransitionsJudged() {
        wire(declaredRegistry());
        observe("e1", "BOOK_ROOM_REQUESTED", 0);
        observe("e2", "ROOM_BOOKED", 1);
        observe("e3", "TAKE_PAYMENT_REQUESTED", 2);
        observe("e4", "PAYMENT_ACCEPTED", 3);
        observe("e5", "ROOM_REJECTED", 4);

        // PAYMENT_ACCEPTED ended the run, so the rejection after it is judged as what it is --
        // an observation after the ending -- and not a second time as a handoff.
        assertThat(standingTypes()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    /**
     * L22: BOOK_ROOM → TAKE_PAYMENT is declared for SUCCESS, and a rejected room is declared
     * to end the run. Payment requested after the rejection contradicts both.
     */
    @Test
    void paymentRequestedAfterARejectedRoomIsAnInvalidTransition() {
        wire(declaredRegistry());
        observe("e1", "BOOK_ROOM_REQUESTED", 0);
        observe("e2", "ROOM_REJECTED", 1);
        observe("e3", "TAKE_PAYMENT_REQUESTED", 2);

        // The rejection ended the run; the payment request is the observation after it. The
        // graph verdict (BOOK_ROOM -> TAKE_PAYMENT on FAILED is INVALID) is WorkflowSpecTest's.
        assertThat(standingTypes()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    /** Judging them costs nothing on the paths the system really takes. */
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

    // ── L21: events a declared validEventTypes map leaves out ─────────────────

    /**
     * ORDER_SAGA declares PAYMENT_STARTED but its map does not list it, so a run that went
     * quiet right after payment began was held to the 60s default, not PROCESS_PAYMENT's 10s.
     */
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

    /** And a transition into such an event is judged: inventory work begun after payment failed. */
    @Test
    void aTransitionIntoAnEventTheMapOmitsIsJudged() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e2", "PAYMENT_FAILED", 1);
        observe("e3", "INVENTORY_STARTED", 2);

        // Judged at all -- it used to be skipped as an event outside the map -- and judged as
        // work begun after the run had failed.
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

    /** @param declaredWorkflow what the source mapping says these events belong to */
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
