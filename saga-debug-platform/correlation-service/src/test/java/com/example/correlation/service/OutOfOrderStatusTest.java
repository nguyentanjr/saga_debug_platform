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

/**
 * What a run's status is, when the platform did not read its events in the order they
 * happened.
 *
 * <h2>The defect these were written against</h2>
 * Status was folded in <em>processing</em> order, one arrival at a time, over the status
 * stored from the last arrival. {@code WorkflowSpec.statusAfter} answers absolutely —
 * {@code INVENTORY_RESERVED} means COMPLETED whatever preceded it — so folding in
 * processing order made the stored status "whatever the most recently read event implies".
 *
 * <p>Measured on this code before the change, with the three events of one healthy order
 * saga and nothing else varied but the order they were handed over:
 *
 * <pre>
 *   read ORDER_CREATED, PAYMENT_COMPLETED, INVENTORY_RESERVED -> COMPLETED
 *   read ORDER_CREATED, INVENTORY_RESERVED, PAYMENT_COMPLETED -> IN_PROGRESS
 * </pre>
 *
 * <p>Same run, same evidence, two different answers — and the second came with a
 * {@code STATE_REGRESSION} finding (COMPLETED to IN_PROGRESS) and a
 * {@code FORBIDDEN_POST_TERMINAL} finding against {@code PAYMENT_COMPLETED}, an event that
 * had happened <em>before</em> the completion it was accused of following. All three
 * described the observer's read order and charged them to the observed system. The end time
 * was the instant of whichever event arrived last, and a run reported IN_PROGRESS while
 * carrying an end time.
 *
 * <p>The fix is to reconstruct the status from all the evidence in hand rather than advance
 * it by whichever event arrived, so the tests below are mostly one assertion made many
 * ways: the answer is a function of the events, not of their arrival order.
 *
 * @see EvidenceTimeline
 */
class OutOfOrderStatusTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    /** One healthy order saga, in the order it happened. */
    private static final List<Observation> HAPPY_PATH = List.of(
            new Observation("e1", "ORDER_CREATED", 0),
            new Observation("e2", "PAYMENT_COMPLETED", 1),
            new Observation("e3", "INVENTORY_RESERVED", 2));

    /** One order saga that failed at the last step and was compensated. */
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

        // The real registry: what the workflow can prove about the order of two events is
        // exactly what is under test, so a stub would only test the stub.
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    // ── A. events read in the order they happened ─────────────────────────────

    @Test
    void aRunReadInOrderReadsAsWhatItWas() {
        observeAll(HAPPY_PATH);

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getStartTime()).isEqualTo(T0);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    // ── B. events read backwards ──────────────────────────────────────────────

    /**
     * The whole run delivered in reverse. Nothing about the run changed, so nothing the
     * platform says about it may change either — status, start and end included.
     */
    @Test
    void aRunReadBackwardsReadsTheSameAsOneReadForwards() {
        observeAll(reversed(HAPPY_PATH));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getStartTime()).isEqualTo(T0);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    // ── C. partially out of order ─────────────────────────────────────────────

    /**
     * The failing case as it was measured. The completion is read before the middle step,
     * and the middle step used to overwrite it: the run finished at IN_PROGRESS.
     */
    @Test
    void aCompletionReadBeforeTheStepBeforeItStillEndsTheRun() {
        observe(HAPPY_PATH.get(0));
        observe(HAPPY_PATH.get(2));
        observe(HAPPY_PATH.get(1));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    /**
     * The general statement the three cases above are instances of: the answer is a
     * function of the evidence alone. Every one of the 120 delivery orders of a five-event
     * compensating run has to produce the same reading.
     */
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

    // ── D. the same event delivered twice ─────────────────────────────────────

    /**
     * A redelivery is recognised and folded in nowhere. Reconstructing from the whole
     * sequence must not turn the duplicate-suppression check into something that runs
     * after the event has already been counted.
     */
    @Test
    void aRedeliveredEventChangesNothingAndIsReported() {
        observeAll(HAPPY_PATH);
        observe(HAPPY_PATH.get(1));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
        assertThat(events).hasSize(3);
        // No transport reference on these observations, so the two cannot be told apart
        // and the finding stands exactly as it did before transport refs existed.
        verify(anomalyDetectionService)
                .detectDuplicateEvent(RUN.canonical(), "e2", "PAYMENT_COMPLETED", null, null);
    }

    /** The whole run replayed from the start: an at-least-once redelivery of everything. */
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

    // ── E. an event that arrives long after the rest ──────────────────────────

    /**
     * A late arrival is an event that happened early. Its instant belongs where it
     * happened, and not to the run's most recent activity — the platform's idea of how far
     * the run has got must not walk backwards, or the stuck-run pass reports a healthy run
     * for standing still.
     */
    @Test
    void aLateArrivalIsNotTreatedAsTheNewestThingToHaveHappened() {
        observe(HAPPY_PATH.get(0));
        observe(HAPPY_PATH.get(2));
        Instant lastUpdatedBefore = run().getLastUpdated();

        observe(HAPPY_PATH.get(1));

        assertThat(run().getLastUpdated()).isEqualTo(lastUpdatedBefore);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    /** And it can reveal that the run started earlier than anything seen so far. */
    @Test
    void aLateStartEventMovesTheRunsBeginningEarlier() {
        observe(HAPPY_PATH.get(1));
        observe(HAPPY_PATH.get(2));
        assertThat(run().getStartTime()).isEqualTo(T0.plusSeconds(1));

        observe(HAPPY_PATH.get(0));

        assertThat(run().getStartTime()).isEqualTo(T0);
    }

    // ── F. observations the clock cannot separate ─────────────────────────────

    /**
     * A producer that stamps a whole burst identically, or a source with no clock of its
     * own falling back to delivery time. The clock says nothing, so the workflow is asked:
     * creating the order is declared upstream of reserving the inventory, so it happened
     * first whatever order the events were read in.
     */
    @Test
    void whenTheClockCannotSeparateEventsTheWorkflowOrdersThem() {
        observe(new Observation("e3", "INVENTORY_RESERVED", 0));
        observe(new Observation("e1", "ORDER_CREATED", 0));
        observe(new Observation("e2", "PAYMENT_COMPLETED", 0));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    /**
     * And when the workflow cannot order them either, nothing is claimed.
     *
     * <p>Reserving inventory and refunding the payment sit on branches the graph does not
     * connect, so with one instant between them there is no evidence for which came last —
     * and they imply opposite outcomes. Picking one would publish a run's ending on the
     * strength of a tie-break.
     */
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

    /** A run whose order is decided raises no such finding. */
    @Test
    void aDecidableRunRaisesNoIndeterminacyFinding() {
        observeAll(reversed(HAPPY_PATH));

        verify(anomalyDetectionService, never())
                .detectIndeterminateOrder(anyString(), any());
    }

    // ── G. a compensation read out of order ───────────────────────────────────

    /**
     * The compensation branch delivered backwards. Reading it in arrival order stopped at
     * COMPENSATING — the refund acknowledgement was overwritten by the failure that had
     * caused it — and a compensated run was reported as one still compensating.
     */
    @Test
    void aCompensationReadBackwardsStillReachesCompensated() {
        observeAll(reversed(COMPENSATED_PATH));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(4));
    }

    /** Interleaved rather than reversed: the acknowledgement read before the failure. */
    @Test
    void aCompensationAcknowledgementReadEarlyStillEndsTheRun() {
        observe(COMPENSATED_PATH.get(0));
        observe(COMPENSATED_PATH.get(4));
        observe(COMPENSATED_PATH.get(2));
        observe(COMPENSATED_PATH.get(1));
        observe(COMPENSATED_PATH.get(3));

        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    // ── H. a terminal state and an event around it ────────────────────────────

    /**
     * An event that really did happen after the run ended is still reported, and reported
     * against the observation that ended it rather than against the platform's state of
     * mind at the time.
     */
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

    /**
     * An event that merely <em>arrived</em> after the run's completion, having happened
     * before it, is not. This was the false positive: on a partitioned stream every
     * reordered delivery accused the observed system of emitting events after its own
     * terminal state.
     */
    @Test
    void anEventThatOnlyArrivedAfterTheEndIsNotReportedAsHappeningAfterIt() {
        observe(HAPPY_PATH.get(0));
        observe(HAPPY_PATH.get(2));

        observe(HAPPY_PATH.get(1));

        verify(anomalyDetectionService, never())
                .detectForbiddenPostTerminal(anyString(), any(), any(), any());
    }

    // ── regressions: the real ones, and the ones that were not ────────────────

    /** Reading a run backwards is not the run going backwards. */
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

    /**
     * A run that really did move backwards is still caught: it completed, and then a step
     * that had already succeeded reported a failure. Nothing about read order can explain
     * that, so it is a claim about the observed system.
     */
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

    /** And it is caught however the three events happen to be delivered. */
    @Test
    void aRealRegressionIsFoundWhicheverOrderItIsReadIn() {
        observe(new Observation("e3", "PAYMENT_FAILED", 2));
        observe(new Observation("e1", "ORDER_CREATED", 0));
        observe(new Observation("e2", "INVENTORY_RESERVED", 1));

        verify(anomalyDetectionService).detectStateRegression(anyString(), any());
    }

    // ── the finding that is about the two orders differing ────────────────────

    /**
     * OUT_OF_ORDER survives all of this, and must. Every other detector was moved onto the
     * reconstructed order precisely so that arrival order stops leaking into claims about
     * the observed system — but this one detector's subject <em>is</em> arrival order
     * disagreeing with event time, so it is still handed the events unsorted.
     */
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
        // Still in arrival order, which is the only order this detector can see the
        // discrepancy in.
        assertThat(lastCall).extracting(SagaEvent::getEventId)
                .containsExactly("e1", "e3", "e2");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

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

    // ── F. clocks that disagree with the declared causation ───────────────────

    /**
     * F28, as delivered on 2026-10-04 (SC01 run 1): the reservation was read first, then the
     * order, then the payment, and the reservation's own stamp was 0.77s before both. With
     * only the reservation and the order in hand nothing links them, so the clock orders the
     * pair and the order reads as arriving after the run had ended. The payment closes the
     * causal chain; that finding no longer holds and is withdrawn, and the run ends COMPLETED.
     */
    @Test
    void aFindingTheClocksSupportedIsWithdrawnWhenTheCausalChainCloses() {
        Instant base = Instant.parse("2026-10-03T19:33:21Z");
        observeCaused("inventory", "INVENTORY_RESERVED", "payment", base.plusMillis(787));
        observeCaused("order", "ORDER_CREATED", null, base.plusMillis(1517));
        verify(anomalyDetectionService).detectForbiddenPostTerminal(eq(RUN.canonical()), any(), any(), any());

        observeCaused("payment", "PAYMENT_COMPLETED", "order", base.plusMillis(1559));

        // Raised on the two-event prefix, then withdrawn once the chain closed (a withdrawal
        // asked while nothing stands is a no-op, so it may also run on the first event).
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
