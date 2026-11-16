package com.example.correlation.service;

import com.example.correlation.domain.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MissingTerminalWithdrawalTest {
    private static final Instant T0 = Instant.parse("2026-09-01T10:00:00Z");
    private static final ExecutionKey RUN = ExecutionKey.of("shop", "ORD-1");

    @Test
    void theEndingWithdrawsMissingTerminalAndKeepsTheSilence() {
        ReplayPipeline p = new ReplayPipeline();
        deliver(p, "e1", "ORDER_CREATED", "CREATE_ORDER", 0);
        deliver(p, "e2", "PAYMENT_COMPLETED", "PROCESS_PAYMENT", 1);
        raise(p, AnomalyType.SAGA_STUCK);
        raise(p, AnomalyType.MISSING_TERMINAL_EVENT);

        deliver(p, "e3", "INVENTORY_RESERVED", "RESERVE_INVENTORY", 150);

        ReplayPipeline.Outcome o = p.outcome(RUN.canonical());
        assertThat(o.sagaStatus).isEqualTo("COMPLETED");
        assertThat(o.anomalies).containsExactly("SAGA_STUCK");
        assertThat(o.retracted).isEqualTo(1);
        assertThat(o.candidateRules).contains("R06:STRONG_EVIDENCE");
    }

    @Test
    void aRunStillWithoutItsEndingKeepsTheFinding() {
        ReplayPipeline p = new ReplayPipeline();
        deliver(p, "e1", "ORDER_CREATED", "CREATE_ORDER", 0);
        raise(p, AnomalyType.MISSING_TERMINAL_EVENT);
        deliver(p, "e2", "PAYMENT_COMPLETED", "PROCESS_PAYMENT", 150);

        assertThat(p.outcome(RUN.canonical()).anomalies).containsExactly("MISSING_TERMINAL_EVENT");
    }

    private static void deliver(ReplayPipeline p, String id, String type, String step, int second) {
        Instant at = T0.plusSeconds(second);
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity("shop", RUN, id, null, CorrelationBasis.DECLARED_KEY),
                null, type, step, "svc", at, "{}", at, IngestedEvent.Causality.NONE, null)));
    }

    private static void raise(ReplayPipeline p, AnomalyType type) {
        Anomaly a = new Anomaly();
        a.setSagaId(RUN.canonical());
        a.setAnomalyType(type);
        a.setDetectedAt(T0.plusSeconds(120));
        p.anomalyRepo.save(a);
    }
}
