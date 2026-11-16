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

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    @Test
    void aRunCreatedByANonStartEventResolvesWhenItsStartEventArrives() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        assertThat(run().getWorkflowType()).isEqualTo(SagaInstance.UNRESOLVED);

        observe("e2", "ORDER_CREATED", T0);

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

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

    @Test
    void findingsMadeBeforeTheWorkflowWasKnownAreWithdrawn() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);

        verify(anomalyDetectionService).retractAbsenceClaims(
                org.mockito.ArgumentMatchers.eq(RUN.canonical()),
                org.mockito.ArgumentMatchers.contains("ORDER_SAGA"));
    }

    @Test
    void learningTheWorkflowIsNotReportedAsAStateRegression() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);

        verify(anomalyDetectionService, never()).detectStateRegression(
                anyString(), org.mockito.ArgumentMatchers.notNull());
    }

    @Test
    void aResolvedWorkflowIsNotUndoneByALaterAmbiguousEvent() {
        observe("e1", "ORDER_CREATED", T0);
        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");

        observe("e2", "SOMETHING_THIS_PLATFORM_HAS_NEVER_HEARD_OF", T0.plusSeconds(1));

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

    @Test
    void repeatedAmbiguousEventsDoNotDegradeAResolvedWorkflow() {
        observe("e1", "ORDER_CREATED", T0);
        observe("e2", "AUDIT_PING", T0.plusSeconds(1));
        observe("e3", "AUDIT_PING_2", T0.plusSeconds(2));
        observe("e4", "AUDIT_PING_3", T0.plusSeconds(3));

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

    @Test
    void anExecutionWithNoRecognisableEventStaysUnresolved() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "PAYMENT_COMPLETED", T0.plusSeconds(1));

        assertThat(run().getWorkflowType()).isEqualTo(SagaInstance.UNRESOLVED);
    }

    @Test
    void aReplayedEventDoesNotChangeTheResolvedWorkflow() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);
        SagaStatus afterFirstPass = run().getStatus();

        observe("e2", "ORDER_CREATED", T0);

        assertThat(run().getWorkflowType()).isEqualTo("ORDER_SAGA");
        assertThat(run().getStatus()).isEqualTo(afterFirstPass);

        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e2", "ORDER_CREATED", null, null);
    }

    @Test
    void findingsAreWithdrawnOnceNotOnEverySubsequentEvent() {
        observe("e1", "INVENTORY_FAILED", T0.plusSeconds(2));
        observe("e2", "ORDER_CREATED", T0);
        observe("e3", "PAYMENT_COMPLETED", T0.plusSeconds(1));

        verify(anomalyDetectionService, org.mockito.Mockito.times(1))
                .retractAbsenceClaims(anyString(), anyString());
    }

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
