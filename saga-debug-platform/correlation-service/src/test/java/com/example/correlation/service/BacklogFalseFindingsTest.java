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

        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusSeconds(100), Instant.now());

        assertThat(standing(p)).contains(AnomalyType.SAGA_STUCK);
    }

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

        p.instances.values().forEach(s -> s.setStatus(SagaStatus.IN_PROGRESS));

        p.detector.detectStuckSagas();

        assertThat(standing(p)).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    @Test
    void missingTerminalPassSkipsARunThatEndedAfterTheRunListWasRead() {
        ReplayPipeline p = new ReplayPipeline();
        when(p.sagas.findByStatusInAndLastUpdatedBefore(any(), any(), any())).thenAnswer(c ->
                p.instances.values().stream().filter(s -> s.getStatus() == SagaStatus.IN_PROGRESS).toList());
        when(p.eventRepo.countBySagaId(anyString())).thenAnswer(c ->
                p.events.stream().filter(e -> e.getSagaId().equals(c.getArgument(0))).count());
        Instant t0 = Instant.now().minusSeconds(3600);
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);
        deliver(p, "e3", "INVENTORY_RESERVED", t0.plusMillis(1000), t0);
        p.instances.values().forEach(s -> s.setStatus(SagaStatus.IN_PROGRESS));

        p.detector.detectMissingTerminalEvents();

        assertThat(standing(p)).doesNotContain(AnomalyType.MISSING_TERMINAL_EVENT);
    }

    @Test
    void silenceIsCountedFromTheLatestArrivalNotTheLatestStamp() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);

        deliver(p, "e1", "ORDER_CREATED", t0, Instant.now());

        p.detector.detectStuckSagas();

        assertThat(standing(p)).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    @Test
    void stuckCitesTheLatestStepNotTheLatestArrival() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now().minusSeconds(120);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0);
        deliver(p, "e1", "ORDER_CREATED", t0, t0.plusSeconds(1));

        p.detector.detectStuckSagas();

        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK)
                .singleElement()
                .satisfies(a -> assertThat(a.getEvidenceEventIds()).containsExactly("e2"));
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

    @Test
    void ingestLocksTheRunItUpdates() {
        ReplayPipeline p = new ReplayPipeline();
        Instant t0 = Instant.now();
        deliver(p, "e1", "ORDER_CREATED", t0, t0);
        deliver(p, "e2", "PAYMENT_COMPLETED", t0.plusMillis(500), t0.plusMillis(500));

        verify(p.sagas, atLeastOnce()).findByIdForUpdate(RUN.canonical());
    }

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
