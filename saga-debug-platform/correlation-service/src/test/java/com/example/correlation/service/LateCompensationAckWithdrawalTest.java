package com.example.correlation.service;

import com.example.correlation.domain.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LateCompensationAckWithdrawalTest {
    private static final Instant T0 = Instant.parse("2026-10-11T23:14:00Z");
    private static final ExecutionKey RUN = ExecutionKey.of("shop", "ORD-1");

    @Test
    void aLateAcknowledgementWithdrawsTheFinding() {
        ReplayPipeline p = compensationStartedAndReported();

        deliver(p, "e5", "PAYMENT_REFUNDED", "REFUND_PAYMENT", 60);

        ReplayPipeline.Outcome o = p.outcome(RUN.canonical());
        assertThat(o.sagaStatus).isEqualTo("COMPENSATED");
        assertThat(o.anomalies).doesNotContain("MISSING_COMPENSATION_ACK");
        assertThat(o.retracted).isEqualTo(1);
        assertThat(o.candidateRules).noneMatch(rule -> rule.contains("STRONG") && rule.startsWith("R10"));
        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.MISSING_COMPENSATION_ACK)
                .singleElement()
                .satisfies(a -> assertThat(a.getRetractionReason()).contains("acknowledgement was observed"));
    }

    @Test
    void anotherObservationWithoutTheAcknowledgementKeepsIt() {
        ReplayPipeline p = compensationStartedAndReported();

        deliver(p, "e5", "REFUND_PAYMENT", "REFUND_PAYMENT", 60);

        assertThat(p.outcome(RUN.canonical()).anomalies).contains("MISSING_COMPENSATION_ACK");
    }

    private static ReplayPipeline compensationStartedAndReported() {
        ReplayPipeline p = new ReplayPipeline();
        deliver(p, "e1", "ORDER_CREATED", "CREATE_ORDER", 0);
        deliver(p, "e2", "PAYMENT_COMPLETED", "PROCESS_PAYMENT", 1);
        deliver(p, "e3", "INVENTORY_FAILED", "RESERVE_INVENTORY", 2);
        deliver(p, "e4", "REFUND_PAYMENT", "REFUND_PAYMENT", 3);
        Anomaly a = new Anomaly();
        a.setSagaId(RUN.canonical());
        a.setAnomalyType(AnomalyType.MISSING_COMPENSATION_ACK);
        a.setDetectedAt(T0.plusSeconds(40));
        a.setEvidenceEventIds(List.of("e4"));
        p.anomalyRepo.save(a);
        return p;
    }

    private static void deliver(ReplayPipeline p, String id, String type, String step, int second) {
        Instant at = T0.plusSeconds(second);
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity("shop", RUN, id, null, CorrelationBasis.DECLARED_KEY),
                null, type, step, "svc", at, "{}", at, IngestedEvent.Causality.NONE, null)));
    }
}
