package com.example.correlation.service;

import com.example.correlation.domain.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * EXT-OVERLOAD-FALSE-FINDINGS: findings raised while the observer is behind its input.
 *
 * <p>Bench 2026-10-21 (adc697a, 1000 ev/s): after the backlog drained, 39 SAGA_STUCK stood on
 * runs that had completed within a second by the source's stamps, and 61 COMPENSATION_MISSING
 * on runs that were COMPENSATED. Each was true of the evidence in hand when raised; the
 * missing events were unread, not absent.
 */
class BacklogFalseFindingsTest {

    private static final ExecutionKey RUN = ExecutionKey.of("default", "S-1");

    @Test
    void stuckWithdrawnWhenTheNextEventHappenedInTimeButWasReadLate() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        p.detector.detectStuckSagas();
        assertThat(standing(p)).contains(AnomalyType.SAGA_STUCK);

        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), Instant.now());

        assertThat(standing(p)).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    @Test
    void stuckStandsWhenTheSourceItselfWasSilent() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        p.detector.detectStuckSagas();

        // Stamped 100s after the order: the step's 10s timeout really was exceeded (R06 case).
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusSeconds(100), Instant.now());

        assertThat(standing(p)).contains(AnomalyType.SAGA_STUCK);
    }

    @Test
    void stuckPassSkipsARunThatEndedAfterTheRunListWasRead() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);
        deliver(p, "e3", "INVENTORY_RESERVED", t0.plusMillis(1000), t0);
        // The pass's run list still says in progress: the ending was ingested after it was read.
        p.instances.values().forEach(s -> s.setStatus(SagaStatus.IN_PROGRESS));

        p.detector.detectStuckSagas();

        assertThat(standing(p)).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    @Test
    void silenceIsCountedFromTheLatestArrivalNotTheLatestStamp() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);
        // Earlier-stamped, read just now from a lagging partition.
        deliver(p, "e1", "ORDER_CREATED", t0, Instant.now());

        p.detector.detectStuckSagas();

        assertThat(standing(p)).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    @Test
    void compensationMissingWithdrawnWhenTheStartArrivesLate() {
        ReplayPipeline p = new ReplayPipeline();
        when(p.sagas.findByStatusInAndLastUpdatedBefore(any(), any(), any())).thenAnswer(c ->
                p.instances.values().stream().filter(s -> s.getStatus() == SagaStatus.COMPENSATING).toList());
        Instant t0 = Instant.now().minusSeconds(600);
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);
        deliver(p, "e3", "INVENTORY_FAILED", t0.plusMillis(1000), t0);
        p.detector.detectCompensationMissing();
        assertThat(standing(p)).contains(AnomalyType.COMPENSATION_MISSING);

        deliver(p, "e4", "REFUND_PAYMENT", t0.plusMillis(1500), Instant.now());

        assertThat(standing(p)).doesNotContain(AnomalyType.COMPENSATION_MISSING);
    }

    private static List<AnomalyType> standing(ReplayPipeline p) {
        return p.anomalies.stream().filter(a -> a.getRetractedAt() == null).map(Anomaly::getAnomalyType).toList();
    }

    private static void deliver(ReplayPipeline p, String id, String type, Instant at, Instant received) {
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity("default", RUN, id, null, CorrelationBasis.DECLARED_KEY),
                "ORDER_SAGA", type, null, "svc", at, "{}", received, IngestedEvent.Causality.NONE, null)));
    }
}
