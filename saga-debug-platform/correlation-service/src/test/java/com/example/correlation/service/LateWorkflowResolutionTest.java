package com.example.correlation.service;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Which workflow a run belongs to, when its events are not read in the order they happened.
 *
 * <h2>The limitation these were written against</h2>
 * One execution's events are spread over several topics and read by several threads, so
 * the first event <em>observed</em> is not always the first event that <em>happened</em>.
 * An execution created by {@code INVENTORY_FAILED} — an event no specification declares as
 * a start — was filed as {@code UNRESOLVED}, and the workflow type was only ever written
 * when the execution row was created. Its {@code ORDER_CREATED} then arrived, resolution
 * correctly answered {@code ORDER_SAGA}, and the answer was thrown away.
 *
 * <p>Observed on a clean deployment: a compensating run of the demo services stopped at
 * {@code IN_PROGRESS}/{@code UNRESOLVED} instead of {@code COMPENSATED}, carrying
 * {@code SAGA_STUCK} and {@code ORPHAN_EVENT}. The observed system had compensated
 * correctly. All three findings described the observer's own read order.
 *
 * <p>So the workflow type is an inference later evidence can improve. These tests pin down
 * both halves of that: that it improves, and that it never degrades.
 */
class LateWorkflowResolutionTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    private Map<String, SagaInstance> instances;
    private List<SagaEvent> events;

    private SagaInstanceRepository sagaInstanceRepository;
    private SagaEventRepository sagaEventRepository;
    private AnomalyDetectionService anomalyDetectionService;
    private CorrelationService correlationService;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        events = new ArrayList<>();

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

        anomalyDetectionService = mock(AnomalyDetectionService.class);

        // The real registry, loaded from the classpath: resolution is what is under test,
        // so stubbing it would only test the stub.
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    // ── the case this exists for ──────────────────────────────────────────────

    /**
     * The exact sequence seen on a clean deployment: the compensation branch is read first
     * and the start event last.
     */
    @Test
    void aRunCreatedByANonStartEventResolvesWhenItsStartEventArrives() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        assertThat(run().getWorkflowType()).isEqualTo(SagaInstance.UNRESOLVED);

        observe("e2", "ORDER_CREATED", T0);

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

    /**
     * And the run reads as what it was. Status was derived while nothing could judge it, so
     * the stored events are replayed once the specification is known — otherwise a
     * compensating run stays {@code IN_PROGRESS} forever and the platform reports a saga
     * that never finished.
     */
    @Test
    void theCompensationIsReconstructedAfterTheWorkflowIsKnown() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "PAYMENT_REFUNDED", T0.plusSeconds(4));
        observe("e3", "PAYMENT_COMPLETED", T0.plusSeconds(1));
        observe("e4", "REFUND_PAYMENT", T0.plusSeconds(3));
        observe("e5", "ORDER_CREATED", T0);

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    /**
     * Findings raised while nothing could judge the run are withdrawn, not left standing.
     * They asserted absences about a run no specification had been applied to.
     */
    @Test
    void findingsMadeBeforeTheWorkflowWasKnownAreWithdrawn() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);

        verify(anomalyDetectionService).retractAbsenceClaims(
                org.mockito.ArgumentMatchers.eq(RUN.canonical()),
                org.mockito.ArgumentMatchers.contains("ORDER_SAGA"));
    }

    /**
     * The status the run had while unjudgeable is not a state it was ever in, so a
     * difference from it is not a regression. Reporting one would be the observer
     * describing the moment it learned something as a change in the observed system.
     */
    @Test
    void learningTheWorkflowIsNotReportedAsAStateRegression() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);

        verify(anomalyDetectionService, never()).detectStateRegression(
                anyString(), org.mockito.ArgumentMatchers.notNull());
    }

    // ── it improves, and only improves ────────────────────────────────────────

    /**
     * A run that resolved on its first event keeps its workflow when a later event says
     * nothing. Otherwise a run would flicker between workflows with its arrival order.
     */
    @Test
    void aResolvedWorkflowIsNotUndoneByALaterAmbiguousEvent() {
        observe("e1", "ORDER_CREATED", T0);
        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");

        // An event no specification declares at all.
        observe("e2", "SOMETHING_THIS_PLATFORM_HAS_NEVER_HEARD_OF", T0.plusSeconds(1));

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

    /** Several unrecognisable events do not accumulate into a downgrade either. */
    @Test
    void repeatedAmbiguousEventsDoNotDegradeAResolvedWorkflow() {
        observe("e1", "ORDER_CREATED", T0);
        observe("e2", "AUDIT_PING", T0.plusSeconds(1));
        observe("e3", "AUDIT_PING_2", T0.plusSeconds(2));
        observe("e4", "AUDIT_PING_3", T0.plusSeconds(3));

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

    /**
     * An execution that never sees an event any specification recognises stays unresolved.
     * Nothing is guessed to fill the blank.
     */
    @Test
    void anExecutionWithNoRecognisableEventStaysUnresolved() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "PAYMENT_COMPLETED", T0.plusSeconds(1));

        assertThat(run().getWorkflowType()).isEqualTo(SagaInstance.UNRESOLVED);
    }

    // ── replay and duplicates ─────────────────────────────────────────────────

    /** A redelivered event is recognised and changes nothing about the workflow. */
    @Test
    void aReplayedEventDoesNotChangeTheResolvedWorkflow() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);
        SagaStatus afterFirstPass = run().getStatus();

        observe("e2", "ORDER_CREATED", T0);

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
        assertThat(run().getStatus()).isEqualTo(afterFirstPass);
        // The duplicate was recognised rather than folded in a second time. These
        // observations carry no transport reference, so the platform cannot tell a re-read
        // from a second delivery and keeps its previous meaning: reported.
        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e2", "ORDER_CREATED", null, null);
    }

    /** Resolving happens once; a later event does not withdraw findings all over again. */
    @Test
    void findingsAreWithdrawnOnceNotOnEverySubsequentEvent() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);
        observe("e3", "PAYMENT_COMPLETED", T0.plusSeconds(1));

        verify(anomalyDetectionService, org.mockito.Mockito.times(1))
                .retractAbsenceClaims(anyString(), anyString());
    }

    // ── an event after the run has ended ──────────────────────────────────────

    /**
     * A late event on a run that already reached a terminal state is still filed and still
     * reported as arriving after the end. Resolving a workflow late must not soften that:
     * the event really did arrive, which is a claim about the observed system, not about
     * the observer.
     */
    @Test
    void anEventArrivingAfterATerminalStateIsStillReported() {
        observe("e1", "ORDER_CREATED", T0);
        observe("e2", "PAYMENT_COMPLETED", T0.plusSeconds(1));
        observe("e3", "INVENTORY_RESERVED", T0.plusSeconds(2));
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);

        observe("e4", "INVENTORY_FAILED", T0.plusSeconds(3));

        verify(anomalyDetectionService).detectForbiddenPostTerminal(
                org.mockito.ArgumentMatchers.eq(RUN.canonical()),
                org.mockito.ArgumentMatchers.eq(SagaStatus.COMPLETED),
                org.mockito.ArgumentMatchers.argThat(
                        terminal -> "INVENTORY_RESERVED".equals(terminal.getEventType())),
                org.mockito.ArgumentMatchers.argThat(
                        late -> "e4".equals(late.getEventId())));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    @Test
    void conflictingStartsStopInferenceInEitherDeliveryOrder() {
        for (List<String> order : List.of(List.of("ORDER_CREATED", "APPLICATION_SUBMITTED"),
                List.of("APPLICATION_SUBMITTED", "ORDER_CREATED"))) {
            setUp();
            observe("e1", order.get(0), T0);
            org.mockito.Mockito.clearInvocations(anomalyDetectionService);
            observe("e2", order.get(1), T0.minusSeconds(1));
            assertThat(run().getWorkflowConflicts()).containsExactly("LOAN_APPROVAL", "ORDER_SAGA");
            assertThat(run().getStatus()).isEqualTo(SagaStatus.IN_PROGRESS);
            assertThat(run().getEndTime()).isNull();
            assertThat(events).hasSize(2);
            org.mockito.Mockito.verifyNoInteractions(anomalyDetectionService);
            observe("e3", order.get(0), T0.plusSeconds(1));
            assertThat(run().getWorkflowConflicts()).containsExactly("LOAN_APPROVAL", "ORDER_SAGA");
            org.mockito.Mockito.verifyNoInteractions(anomalyDetectionService);
        }
    }

    @Test
    void lateConflictInvalidatesAPreviouslyConcludedEnding() {
        observe("e1", "ORDER_CREATED", T0);
        observe("e2", "PAYMENT_COMPLETED", T0.plusSeconds(1));
        observe("e3", "INVENTORY_RESERVED", T0.plusSeconds(2));
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        observe("e4", "APPLICATION_SUBMITTED", T0.minusSeconds(1));
        assertThat(run().hasWorkflowConflict()).isTrue();
        assertThat(run().getStatus()).isEqualTo(SagaStatus.IN_PROGRESS);
        assertThat(run().getEndTime()).isNull();
    }

    private SagaInstance run() {
        return instances.get(RUN.canonical());
    }

    private void observe(String eventId, String eventType, Instant when) {
        correlationService.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SOURCE, RUN, eventId, null,
                        CorrelationBasis.DECLARED_KEY),
                null,
                eventType,
                "UNKNOWN",
                "a-service",
                when,
                "{}",
                Instant.now(),
                IngestedEvent.Causality.NONE));
    }
}
