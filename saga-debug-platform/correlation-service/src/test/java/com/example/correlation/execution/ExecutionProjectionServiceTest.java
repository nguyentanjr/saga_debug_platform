package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjection.Operation;
import com.example.correlation.execution.ExecutionProjection.OperationOutcome;
import com.example.correlation.execution.ExecutionProjection.StartProvenance;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionProjectionServiceTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final ExecutionProjectionService service = new ExecutionProjectionService();
    private final WorkflowSpec spec = WorkflowSpecTestFixture.orderSaga();

    @Test
    void unmappedEventDoesNotChangeOperationTiming() {
        List<SagaEvent> withoutNoise = List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e2", "PAYMENT_COMPLETED", "payment-service", T0.plusSeconds(12)),
                event("e3", "INVENTORY_RESERVED", "inventory-service", T0.plusSeconds(13)));

        List<SagaEvent> withNoise = new ArrayList<>(withoutNoise);
        withNoise.add(event("e-noise", "AUDIT_PING", "audit-service", T0.plusSeconds(11)));

        Operation before = paymentOperation(withoutNoise);
        Operation after = paymentOperation(withNoise);

        assertThat(before.duration()).isEqualTo(after.duration());
        assertThat(after.duration().toSeconds()).isEqualTo(12);
        assertThat(after.startEventId()).isEqualTo("e1");
        assertThat(after.endEventId()).isEqualTo("e2");
    }

    @Test
    void unmappedEventIsReportedRatherThanSilentlyDropped() {
        ExecutionProjection projection = project(List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e-noise", "AUDIT_PING", "audit-service", T0.plusSeconds(11))));

        assertThat(projection.unmappedEvents())
                .extracting(SagaEvent::getEventType)
                .containsExactly("AUDIT_PING");
    }

    @Test
    void retriesAreFoldedIntoTheOperationInsteadOfResettingIt() {
        Operation payment = paymentOperation(List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e2", "PAYMENT_RETRY", "payment-service", T0.plusSeconds(3)),
                event("e3", "PAYMENT_RETRY", "payment-service", T0.plusSeconds(6)),
                event("e4", "PAYMENT_RETRY", "payment-service", T0.plusSeconds(9)),
                event("e5", "PAYMENT_FAILED", "payment-service", T0.plusSeconds(11))));

        assertThat(payment.duration().toSeconds()).isEqualTo(11);
        assertThat(payment.attemptCount()).isEqualTo(4);
        assertThat(payment.outcome()).isEqualTo(OperationOutcome.FAILED);
    }

    @Test
    void attemptBoundariesFollowRetryObservations() {
        Operation payment = paymentOperation(List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e2", "PAYMENT_RETRY", "payment-service", T0.plusSeconds(3)),
                event("e3", "PAYMENT_COMPLETED", "payment-service", T0.plusSeconds(8))));

        assertThat(payment.attempts()).hasSize(2);
        assertThat(payment.attempts().get(0).ordinal()).isEqualTo(1);
        assertThat(payment.attempts().get(0).startEventId()).isEqualTo("e1");
        assertThat(payment.attempts().get(0).endEventId()).isEqualTo("e2");
        assertThat(payment.attempts().get(1).endEventId()).isEqualTo("e3");
        assertThat(payment.attempts().get(1).duration().toSeconds()).isEqualTo(5);
    }

    @Test
    void projectionIsInvariantToDeliveryOrder() {
        List<SagaEvent> events = new ArrayList<>(List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e2", "PAYMENT_RETRY", "payment-service", T0.plusSeconds(3)),
                event("e3", "PAYMENT_COMPLETED", "payment-service", T0.plusSeconds(8)),
                event("e4", "INVENTORY_RESERVED", "inventory-service", T0.plusSeconds(9))));

        ExecutionProjection inOrder = project(events);

        List<SagaEvent> shuffled = new ArrayList<>(events);
        Collections.reverse(shuffled);
        ExecutionProjection reversed = project(shuffled);

        assertThat(summarize(reversed)).isEqualTo(summarize(inOrder));
    }

    @Test
    void operationWithoutStartReferenceHasNoDuration() {
        ExecutionProjection projection = project(List.of(
                event("e1", "INVENTORY_RESERVED", "inventory-service", T0.plusSeconds(30))));

        Operation inventory = projection.operation("RESERVE_INVENTORY").orElseThrow();
        assertThat(inventory.startProvenance()).isEqualTo(StartProvenance.NO_START_REFERENCE);
        assertThat(inventory.duration()).isNull();
        assertThat(projection.measurableOperations()).isEmpty();
    }

    @Test
    void inferredStartIsLabelledAsInferred() {
        Operation payment = paymentOperation(List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e2", "PAYMENT_COMPLETED", "payment-service", T0.plusSeconds(4))));

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
        assertThat(payment.hasObservedStart()).isFalse();
    }

    @Test
    void observedStartIsUsedWhenTheWorkflowDeclaresOne() {
        ExecutionProjection projection = project(List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e2", "PAYMENT_COMPLETED", "payment-service", T0.plusSeconds(1)),
                event("e3", "INVENTORY_FAILED", "inventory-service", T0.plusSeconds(2)),
                event("e4", "REFUND_PAYMENT", "order-service", T0.plusSeconds(3)),
                event("e5", "PAYMENT_REFUNDED", "payment-service", T0.plusSeconds(9))));

        Operation refund = projection.operation("REFUND_PAYMENT").orElseThrow();
        assertThat(refund.startProvenance()).isEqualTo(StartProvenance.OBSERVED_START);
        assertThat(refund.duration().toSeconds()).isEqualTo(6);
        assertThat(refund.outcome()).isEqualTo(OperationOutcome.SUCCEEDED);
    }

    @Test
    void operationWithoutTerminalObservationIsInFlight() {
        Operation payment = paymentOperation(List.of(
                event("e1", "ORDER_CREATED", "order-service", T0),
                event("e2", "PAYMENT_RETRY", "payment-service", T0.plusSeconds(3))));

        assertThat(payment.outcome()).isEqualTo(OperationOutcome.IN_FLIGHT);
        assertThat(payment.duration()).isNull();
    }

    @Test
    void withoutASpecNothingIsAttributedToAStep() {
        ExecutionProjection projection = service.project("saga-1",
                List.of(event("e1", "ORDER_CREATED", "order-service", T0)), null);

        assertThat(projection.operations()).isEmpty();
        assertThat(projection.unmappedEvents()).hasSize(1);
    }

    private ExecutionProjection project(List<SagaEvent> events) {
        return service.project("saga-1", events, spec);
    }

    private Operation paymentOperation(List<SagaEvent> events) {
        return project(events).operation("PROCESS_PAYMENT").orElseThrow();
    }

    private static List<String> summarize(ExecutionProjection projection) {
        return projection.operations().stream()
                .map(op -> op.stepId() + "|" + op.startedAt() + "|" + op.endedAt()
                        + "|" + op.startProvenance() + "|" + op.attemptCount())
                .toList();
    }

    static SagaEvent event(String eventId, String eventType, String serviceName, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId("saga-1");
        event.setEventType(eventType);
        event.setServiceName(serviceName);
        event.setTimestamp(timestamp);
        event.setPayload("{}");
        event.setReceivedAt(timestamp);
        return event;
    }
}
