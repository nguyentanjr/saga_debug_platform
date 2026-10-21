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

/**
 * A regression is a claim about an ending, and only about an ending.
 *
 * <h2>What went wrong</h2>
 * {@code STATE_REGRESSION} was raised for any drop down {@link SagaStatus#progressRank}
 * between two consecutive, provably ordered observations. Two things made that unsafe.
 *
 * <p>First, "consecutive" meant consecutive <em>in the evidence in hand</em>, which is not
 * the same as consecutive in the run. On a workflow with two concurrent arms, a run whose
 * payment arm failed sat at COMPENSATING; the stock arm, in flight the whole time and owing
 * nothing to the payment arm, then reported success, which reads as IN_PROGRESS. On the
 * three events observed so far that is a drop. The fourth event — the compensation request,
 * carrying the same instant as the stock reply — inserts itself between them and the drop
 * disappears.
 *
 * <p>Second, the finding is an {@code OCCURRENCE} claim, so it was never revisited. The
 * same four events therefore produced two different diagnoses depending only on when the
 * platform happened to read them: {@code TERMINAL_STATE_OVERWRITE (R04)} read live,
 * {@code COMPENSATION_STEP_FAILURE (R10)} read as a backlog. R10 was the true one.
 *
 * <h2>What holds now</h2>
 * The earlier half of the pair must be terminal. A terminal status is absolute — no later
 * observation can make a run un-ended — so the claim survives any amount of further
 * evidence, which is what makes it safe to file on the partial evidence an observer always
 * has. It is also what R04 is named for.
 *
 * <p>The tests below are one property stated several ways: for a fixed set of events, the
 * finding is the same whatever order they arrive in.
 *
 * @see EvidenceTimeline
 */
class RegressionEvidenceCompletenessTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    /**
     * The shape that produced the false finding, on PARALLEL_FULFILMENT.
     *
     * <p>The inventory arm refuses, which compensates the funds arm; the funds arm answers
     * afterwards anyway, because nothing ordered the two. The release request shares its
     * instant with that answer, exactly as the observed system's did.
     */
    private static final List<Observation> COMPENSATING_PARALLEL_RUN = List.of(
            new Observation("e1", "BASKET_ACCEPTED", 0),
            new Observation("e2", "INVENTORY_REFUSED", 1),
            new Observation("e3", "FUNDS_HELD", 2),
            new Observation("e4", "FUNDS_RELEASE_REQUESTED", 2));

    /** A run that really did contradict its own ending: it completed, then a step failed. */
    private static final List<Observation> OVERWRITTEN_ENDING = List.of(
            new Observation("f1", "ORDER_CREATED", 0),
            new Observation("f2", "INVENTORY_RESERVED", 1),
            new Observation("f3", "PAYMENT_FAILED", 2));

    // ══ the false finding is gone ═════════════════════════════════════════════

    /**
     * The headline, on the prefix that used to raise it. Three of the run's four events,
     * in the order they happened, and no claim is made.
     */
    @Test
    void anArmAnsweringAfterTheOtherArmFailedIsNotARegression() {
        observeAll(COMPENSATING_PARALLEL_RUN.subList(0, 3));

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    /** And the complete run does not raise it either, so the two agree. */
    @Test
    void norIsItOnceTheWholeRunHasArrived() {
        observeAll(COMPENSATING_PARALLEL_RUN);

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    /**
     * The exact sequence from the observed system: the third event, which used to make the
     * claim, and then the fourth, which the complete set shows would have withdrawn it. The
     * result has to be what processing all four together gives.
     */
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

    /**
     * The acceptance property, stated exhaustively: all 24 delivery orders of the four
     * events, every one of them silent. A claim that depends on arrival order is a claim
     * about the observer.
     */
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

    // ══ the genuine finding is untouched ══════════════════════════════════════

    /**
     * A run that completed and then reported a failure at a step that had already
     * succeeded. Nothing about read order explains that, and no later observation can make
     * the completion un-happen, so it is a claim about the observed system.
     */
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

    /** And it is found whichever order the three events arrive in — all six of them. */
    @Test
    void everyDeliveryOrderOfThatRunProducesTheFinding() {
        for (List<Observation> order : permutations(OVERWRITTEN_ENDING)) {
            setUp();
            observeAll(order);

            verify(anomalyDetectionService, atLeastOnce())
                    .detectStateRegression(anyString(), any());
        }
    }

    /**
     * A compensated run that is then contradicted — the shape the synthetic C4 scenario
     * injects. COMPENSATED is terminal, so this stays a finding.
     */
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

    // ══ nothing else moved ════════════════════════════════════════════════════

    /** A healthy run says nothing, read in order or backwards. */
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

    /**
     * A healthy parallel run, both arms succeeding, in either arm order. The fan-out is
     * where the false finding came from, so this pins that the ordinary case stays quiet.
     */
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

    /**
     * The contested-order refusal is unchanged: two observations the evidence cannot order
     * still evidence nothing, terminal or not.
     */
    @Test
    void aContestedPairStillEvidencesNothing() {
        observeAll(List.of(
                new Observation("c1", "BASKET_ACCEPTED", 0),
                new Observation("c2", "FUNDS_REFUSED", 1),
                new Observation("c3", "INVENTORY_HELD", 1)));

        verify(anomalyDetectionService, never()).detectStateRegression(anyString(), any());
    }

    // ── fixture ───────────────────────────────────────────────────────────────

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

    /** Every delivery order of one run's events. */
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
