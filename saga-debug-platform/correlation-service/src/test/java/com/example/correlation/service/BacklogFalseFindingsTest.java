package com.example.correlation.service;

import com.example.correlation.domain.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
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

    /**
     * FTGO kitchen-stop (6879a58): VALIDATE_REQUESTED read 20 ms after TICKET_CREATE_REQUESTED
     * though stamped before it, then 64 s of real silence. The neighbour already in hand
     * withdrew the finding; only what is read after it may.
     */
    @Test
    void stuckStandsWhenOnlyEventsReadBeforeItAreClose() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        deliver(p, "e3", "INVENTORY_STARTED", t0.plusMillis(60), t0.plusMillis(500));
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(2), t0.plusMillis(520));
        p.detector.detectStuckSagas();

        deliver(p, "e4", "INVENTORY_RESERVED", t0.plusSeconds(64), Instant.now());

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

    /** Ingest holds the run the way RCA does; see IngestRcaLockTest for why. */
    @Test
    void ingestLocksTheRunItUpdates() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now();
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0.plusMillis(500));

        verify(p.sagas, atLeastOnce()).findByIdForUpdate(RUN.canonical());
    }

    /**
     * A diagnosis is dated when its evidence was read: an analysis slowed by load must not
     * carry a stamp later than a withdrawal that came after its read.
     */
    @Test
    void aDiagnosisIsDatedWhenItsEvidenceWasRead() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);
        java.util.concurrent.atomic.AtomicReference<Instant> readAt = new java.util.concurrent.atomic.AtomicReference<>();
        when(p.anomalyRepo.findStandingBySagaId(anyString())).thenAnswer(c -> {
            readAt.compareAndSet(null, Instant.now());
            Thread.sleep(50);
            return p.anomalies.stream().filter(a -> a.getSagaId().equals(c.getArgument(0))
                    && a.getRetractedAt() == null).toList();
        });

        com.example.correlation.domain.RcaResult result = p.rca.analyze(RUN.canonical());

        assertThat(result.getCreatedAt()).isBeforeOrEqualTo(readAt.get());
    }

    /**
     * EXT-RCA-STALE-RACE: the analysis read ORPHAN_EVENT standing, the withdrawal committed
     * after that read but carries an earlier stamp. No stamp comparison sees it; the cited
     * finding being withdrawn does.
     */
    @Test
    void aDiagnosisCitingAWithdrawnFindingIsRedone() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(60);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);
        List<com.example.correlation.domain.RcaResult> results = new java.util.ArrayList<>();
        when(p.rcaRepo.save(any())).thenAnswer(c -> { results.add(0, c.getArgument(0)); return c.getArgument(0); });
        when(p.rcaRepo.findBySagaIdOrderByCreatedAtDesc(anyString())).thenAnswer(c -> List.copyOf(results));
        when(p.anomalyRepo.findAllById(any())).thenAnswer(c -> {
            java.util.Set<Long> ids = new java.util.HashSet<>();
            ((Iterable<Long>) c.getArgument(0)).forEach(ids::add);
            return p.anomalies.stream().filter(a -> ids.contains(a.getId())).toList();
        });
        when(p.anomalyRepo.findSagaIdsWithAnomaliesSince(any())).thenReturn(List.of(RUN.canonical()));
        // The sweep reaches analyze through the Spring proxy; the harness has none.
        org.springframework.test.util.ReflectionTestUtils.setField(p.rca, "self", p.rca);

        com.example.correlation.domain.RcaResult first = p.rca.analyze(RUN.canonical());
        assertThat(first.getPrimaryRootCauses()).contains("WRONG_SAGA_CORRELATION_KEY");
        Anomaly orphan = p.anomalies.stream().filter(a -> a.getAnomalyType() == AnomalyType.ORPHAN_EVENT).findFirst().orElseThrow();
        orphan.setRetractedAt(first.getCreatedAt().minusMillis(50));
        when(p.anomalyRepo.findLatestDetectedAtBySagaId(anyString())).thenReturn(orphan.getRetractedAt());

        p.rca.autoAnalyzeNewlyDetectedAnomalies();

        assertThat(results).hasSize(2);
        assertThat(results.get(0).getPrimaryRootCauses()).doesNotContain("WRONG_SAGA_CORRELATION_KEY");
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
