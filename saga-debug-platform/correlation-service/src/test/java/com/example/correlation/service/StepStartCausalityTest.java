package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXT-HOTEL-EVENT-TIME: a step's outcome is not placed before the step's own start.
 *
 * <p>Hotel live (semotpan, 2026-10-21): PAYMENT_REJECTED carried a transport time 0.6 s before
 * the TAKE_PAYMENT_REQUESTED it answered. Ordered by clock the run went TAKE_PAYMENT -> BOOK_ROOM
 * on FAILED and INVALID_TRANSITION stood. Events, stamps and read order as observed.
 */
class StepStartCausalityTest {

    private static final ExecutionKey RUN = ExecutionKey.of("hotel-saga", "6e91cae7");
    private static final Instant T = Instant.parse("2026-10-21T09:52:56Z");

    @Test
    void anOutcomeStampedBeforeItsStepStartIsReadAfterIt() {
        ReplayPipeline p = new ReplayPipeline(declaredRegistry());
        deliver(p, "book", "BOOK_ROOM_REQUESTED", 1116);
        deliver(p, "booked", "ROOM_BOOKED", 1245);
        deliver(p, "take", "TAKE_PAYMENT_REQUESTED", 1330);
        deliver(p, "rejected", "PAYMENT_REJECTED", 758);
        deliver(p, "release", "RELEASE_ROOM_REQUESTED", 1856);
        deliver(p, "cancelled", "ROOM_CANCELLED", 1981);

        assertThat(p.anomalies).filteredOn(a -> a.getRetractedAt() == null)
                .extracting(Anomaly::getAnomalyType)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(p.instances.get(RUN.canonical()).getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    /** With two attempts in hand the specification does not say which one an outcome answers. */
    @Test
    void twoStartsOfOneStepLeaveTheClockInCharge() {
        ReplayPipeline p = new ReplayPipeline(declaredRegistry());
        deliver(p, "book", "BOOK_ROOM_REQUESTED", 1116);
        deliver(p, "booked", "ROOM_BOOKED", 1245);
        deliver(p, "take1", "TAKE_PAYMENT_REQUESTED", 1330);
        deliver(p, "take2", "TAKE_PAYMENT_REQUESTED", 1400);
        deliver(p, "rejected", "PAYMENT_REJECTED", 758);

        assertThat(p.anomalies).filteredOn(a -> a.getRetractedAt() == null)
                .extracting(Anomaly::getAnomalyType)
                .contains(AnomalyType.INVALID_TRANSITION);
    }

    private static void deliver(ReplayPipeline p, String id, String type, int millisAfterT) {
        Instant at = T.plusMillis(millisAfterT);
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity("hotel-saga", RUN, id, null, CorrelationBasis.DECLARED_KEY),
                "ROOM_RESERVATION", type, null, "svc", at, "{}", Instant.now(),
                IngestedEvent.Causality.NONE, null)));
    }

    private static WorkflowRegistry declaredRegistry() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        WorkflowRegistry registry = new WorkflowRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", root.resolve("declarations").toString());
        registry.load();
        return registry;
    }
}
