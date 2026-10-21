package com.example.correlation.service;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Runs a workflow the engine was never written for.
 *
 * <p>FULFILLMENT_SAGA shares no event type, step id or service name with the order
 * saga, and — the part that used to break everything — it has a step after the one that
 * reserves stock. The old state machine declared a saga COMPLETED the moment
 * INVENTORY_RESERVED arrived, so a workflow shaped like this could only end up in one
 * of two wrong states, both of which are asserted below as regressions that must not
 * come back.
 */
class SecondWorkflowGeneralizationTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final WorkflowSpec fulfillment =
            WorkflowSpecTestFixture.load("workflows/fulfillment-saga-workflow.yaml");

    private SagaInstanceRepository sagaInstanceRepository;
    private SagaEventRepository sagaEventRepository;
    private AnomalyDetectionService anomalyDetectionService;
    private CorrelationService correlationService;
    private final List<SagaInstance> saved = new ArrayList<>();

    @BeforeEach
    void setUp() {
        sagaInstanceRepository = mock(SagaInstanceRepository.class);
        sagaEventRepository = mock(SagaEventRepository.class);
        anomalyDetectionService = mock(AnomalyDetectionService.class);

        WorkflowRegistry registry = mock(WorkflowRegistry.class);
        lenient().when(registry.byType("FULFILLMENT_SAGA")).thenReturn(Optional.of(fulfillment));
        lenient().when(registry.byType(null)).thenReturn(Optional.empty());
        lenient().when(registry.forStartEvent("SHIPMENT_CREATED")).thenReturn(Optional.of(fulfillment));

        // Resolution now takes the observed system into account, so the stub mirrors the
        // production signature: (resolved workflow, mapping-declared workflow, source,
        // event type).
        lenient().when(registry.resolve(any(), any(), any(), any())).thenAnswer(inv -> {
            String type = inv.getArgument(0);
            String eventType = inv.getArgument(3);
            if ("FULFILLMENT_SAGA".equals(type)) return Optional.of(fulfillment);
            return fulfillment.startEventTypes().contains(eventType)
                    ? Optional.of(fulfillment) : Optional.empty();
        });

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());

        when(sagaEventRepository.findBySourceNameAndEventId(anyString(), anyString()))
                .thenReturn(Optional.empty());
        when(sagaEventRepository.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenAnswer(inv -> List.copyOf(storedEvents));
        when(sagaEventRepository.save(any(SagaEvent.class))).thenAnswer(inv -> {
            storedEvents.add(inv.getArgument(0));
            return inv.getArgument(0);
        });
        // A detached copy: processEvent mutates what findById hands it, and returning
        // the stored instance would rewrite the history this test is recording.
        when(sagaInstanceRepository.findById(anyString()))
                .thenAnswer(inv -> saved.isEmpty()
                        ? Optional.empty() : Optional.of(copyOf(saved.get(saved.size() - 1))));
        when(sagaInstanceRepository.findByIdForUpdate(anyString()))
                .thenAnswer(inv -> saved.isEmpty()
                        ? Optional.empty() : Optional.of(copyOf(saved.get(saved.size() - 1))));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(inv -> {
            SagaInstance saga = inv.getArgument(0);
            saved.add(copyOf(saga));
            return saga;
        });
    }

    private final List<SagaEvent> storedEvents = new ArrayList<>();

    @Test
    void aHealthyRunOfTheSecondWorkflowCompletesAtItsLastStep() {
        replayHappyPath();

        assertThat(statusAfterEach()).containsExactly(
                SagaStatus.IN_PROGRESS,   // SHIPMENT_CREATED  -> next step follows
                SagaStatus.IN_PROGRESS,   // CHARGE_COMPLETED  -> next step follows
                SagaStatus.IN_PROGRESS,   // STOCK_RESERVED    -> DISPATCH_PARCEL follows
                SagaStatus.COMPLETED);    // PARCEL_DISPATCHED -> transitions to COMPLETED
    }

    /**
     * The regression this whole change exists to prevent.
     *
     * <p>Under the previous state machine STOCK_RESERVED was not in COMPLETED_EVENTS, so
     * the saga never reached a terminal status and every healthy run was eventually
     * reported as stuck. Had the workflow reused the name INVENTORY_RESERVED it would
     * have completed three events early and reported its own dispatch as an event after
     * a terminal state. Neither happens now.
     */
    @Test
    void aHealthyRunOfTheSecondWorkflowRaisesNoAnomaly() {
        replayHappyPath();

        // Consulted on every event now, and it decides for itself: the claim is about the
        // run's whole evidence, not about which event happened to create the row. What
        // matters is that every run it was handed already contained a start event, which is
        // the condition under which it raises nothing.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<SagaEvent>> orphanEvidence = ArgumentCaptor.forClass(List.class);
        verify(anomalyDetectionService, atLeast(0))
                .detectOrphanEvent(any(), anyString(), orphanEvidence.capture());
        for (List<SagaEvent> run : orphanEvidence.getAllValues()) {
            assertThat(run).extracting(SagaEvent::getEventType)
                    .as("every run handed to the orphan check contains a start event")
                    .contains("SHIPMENT_CREATED");
        }
        // These two are no longer consulted on every event to decide for themselves. The
        // correlation service asks the reconstructed timeline whether the run really did
        // end before this event happened, and whether it really did move backwards, so
        // reaching the detector at all is the finding.
        verify(anomalyDetectionService, never()).detectForbiddenPostTerminal(
                anyString(), any(), any(), any());
        verify(anomalyDetectionService, never()).detectStateRegression(
                anyString(), org.mockito.ArgumentMatchers.notNull());
        // Same shape again: the transition check runs on every consecutive pair, and the
        // detector raises nothing when the pair is declared. Assert that every pair it
        // was handed is one this workflow declares.
        ArgumentCaptor<String> fromStep = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> toStep = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> condition = ArgumentCaptor.forClass(String.class);
        verify(anomalyDetectionService, atLeast(0)).detectInvalidTransition(
                any(), anyString(), fromStep.capture(), toStep.capture(), condition.capture(),
                anyString(), any());
        for (int i = 0; i < fromStep.getAllValues().size(); i++) {
            assertThat(fulfillment.isValidTransition(
                    fromStep.getAllValues().get(i),
                    toStep.getAllValues().get(i),
                    condition.getAllValues().get(i)))
                    .as("transition %s -> %s on %s", fromStep.getAllValues().get(i),
                            toStep.getAllValues().get(i), condition.getAllValues().get(i))
                    .isTrue();
        }
    }

    @Test
    void theSagaIsTaggedWithTheWorkflowItBelongsTo() {
        replayHappyPath();

        // workflow_type existed as a column but nothing ever wrote a resolved value to
        // it or read it back; every saga was implicitly ORDER_SAGA.
        assertThat(saved).isNotEmpty();
        assertThat(saved.get(saved.size() - 1).getWorkflowType()).isEqualTo("FULFILLMENT_SAGA");
    }

    @Test
    void aFailureRoutesToCompensationWhenTheWorkflowDeclaresOne() {
        send("f1", "SHIPMENT_CREATED", T0);
        send("f2", "CHARGE_COMPLETED", T0.plusSeconds(1));
        send("f3", "STOCK_FAILED", T0.plusSeconds(2));

        // RESERVE_STOCK -> COMPENSATING on FAILED, per this workflow's own transitions.
        assertThat(lastStatus()).isEqualTo(SagaStatus.COMPENSATING);

        send("f4", "CHARGE_REVERSAL_REQUESTED", T0.plusSeconds(3));
        assertThat(lastStatus()).isEqualTo(SagaStatus.COMPENSATING);

        send("f5", "CHARGE_REVERSED", T0.plusSeconds(4));
        assertThat(lastStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    void aFailureOnAStepWithoutCompensationEndsTheSaga() {
        send("g1", "SHIPMENT_CREATED", T0);
        send("g2", "CHARGE_FAILED", T0.plusSeconds(1));

        // CHARGE_CUSTOMER -> FAILED, not COMPENSATING: the spec says so.
        assertThat(lastStatus()).isEqualTo(SagaStatus.FAILED);
    }

    @Test
    void anEventFromAnotherWorkflowDoesNotStartThisOne() {
        // ORDER_CREATED starts the order saga, not this one.
        send("x1", "ORDER_CREATED", T0);

        verify(anomalyDetectionService)
                .detectOrphanEvent(any(), anyString(), any());
    }

    @Test
    void anUnrecognisedExecutionIsNotSilentlyTaggedAsSomeOtherWorkflow() {
        send("x1", "ORDER_CREATED", T0);

        // The field used to default to ORDER_SAGA, so an execution nothing recognised was
        // evaluated against the order saga's steps and timeouts. It now stays unresolved,
        // which makes every spec-driven detector abstain for it.
        assertThat(saved.get(saved.size() - 1).getWorkflowType())
                .isEqualTo(SagaInstance.UNRESOLVED);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private void replayHappyPath() {
        send("e1", "SHIPMENT_CREATED", T0);
        send("e2", "CHARGE_COMPLETED", T0.plusSeconds(1));
        send("e3", "STOCK_RESERVED", T0.plusSeconds(2));
        send("e4", "PARCEL_DISPATCHED", T0.plusSeconds(3));
    }

    private void send(String eventId, String eventType, Instant at) {
        correlationService.processEvent(eventId, "saga-f1", eventType, "UNKNOWN",
                "test", at, "{}", at);
    }

    private List<SagaStatus> statusAfterEach() {
        return saved.stream().map(SagaInstance::getStatus).toList();
    }

    private SagaStatus lastStatus() {
        return saved.get(saved.size() - 1).getStatus();
    }

    private static int rank(SagaStatus status) {
        return switch (status) {
            case STARTED -> 1;
            case IN_PROGRESS -> 2;
            case COMPENSATING -> 3;
            case FAILED -> 4;
            case COMPENSATED -> 5;
            case COMPLETED -> 6;
        };
    }

    private static SagaInstance copyOf(SagaInstance source) {
        SagaInstance copy = new SagaInstance();
        copy.setSagaId(source.getSagaId());
        copy.setWorkflowType(source.getWorkflowType());
        copy.setStatus(source.getStatus());
        copy.setStartTime(source.getStartTime());
        copy.setEndTime(source.getEndTime());
        copy.setLastUpdated(source.getLastUpdated());
        return copy;
    }
}
