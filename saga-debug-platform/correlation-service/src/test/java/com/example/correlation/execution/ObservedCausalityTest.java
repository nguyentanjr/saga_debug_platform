package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjection.Operation;
import com.example.correlation.execution.ExecutionProjection.StartProvenance;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ObservedCausalityTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final ExecutionProjectionService service = new ExecutionProjectionService();
    private final WorkflowSpec orderSaga = WorkflowSpecTestFixture.orderSaga();
    private final WorkflowSpec parallel =
            WorkflowSpecTestFixture.load("workflows/parallel-checkout-workflow.yaml");

    @Test
    void areportedCauseIsPreferredOverTheInferredPredecessor() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "e1"))
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(payment.startEventId()).isEqualTo("e1");
        assertThat(payment.duration().toSeconds()).isEqualTo(4);
    }

    @Test
    void withoutAReportedCauseTheInferenceIsUsedAndLabelledAsSuch() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)))
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
        assertThat(payment.duration().toSeconds()).isEqualTo(4);
    }

    @Test
    void parentSpanIdIsUsedWhenNoDirectCauseIsReported() {
        SagaEvent cause = event("e1", "ORDER_CREATED", T0);
        cause.setSpanId("aaaa1111");

        SagaEvent effect = event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4));
        effect.setParentSpanId("aaaa1111");

        Operation payment = project(orderSaga, cause, effect)
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(payment.startEventId()).isEqualTo("e1");
    }

    @Test
    void aCauseOutsideThisExecutionResolvesToNothingRatherThanToAGuess() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "from-another-saga"))
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
    }

    @Test
    void areportedCauseIdentifiesWhichArmTriggeredTheJoin() {
        ExecutionProjection projection = project(parallel,
                event("e1", "CART_OPENED", T0),
                event("e2", "SEATS_RESERVED", T0.plusSeconds(3)),
                event("e3", "FRAUD_CHECK_PASSED", T0.plusSeconds(8)),
                causedBy(event("e4", "ORDER_SETTLED", T0.plusSeconds(11)), "e2"));

        Operation settle = projection.operation("SETTLE_ORDER").orElseThrow();

        assertThat(settle.startProvenance()).isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(settle.startEventId()).isEqualTo("e2");
        assertThat(settle.duration().toSeconds()).isEqualTo(8);
    }

    @Test
    void anEventCitingItselfIsIgnored() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "e2"))
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
    }

    @Test
    void aMixOfReportedAndUnreportedLinksIsHandledPerOperation() {
        ExecutionProjection projection = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "e1"),
                event("e3", "INVENTORY_RESERVED", T0.plusSeconds(6)));

        assertThat(projection.operation("PROCESS_PAYMENT").orElseThrow().startProvenance())
                .isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(projection.operation("RESERVE_INVENTORY").orElseThrow().startProvenance())
                .isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
    }

    private ExecutionProjection project(WorkflowSpec spec, SagaEvent... events) {
        return service.project("saga-c1", List.of(events), spec);
    }

    private static SagaEvent causedBy(SagaEvent event, String causationId) {
        event.setCausationId(causationId);
        return event;
    }

    private static SagaEvent event(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId("saga-c1");
        event.setEventType(eventType);
        event.setServiceName("test");
        event.setTimestamp(timestamp);
        event.setPayload("{}");
        event.setReceivedAt(timestamp);
        return event;
    }
}
