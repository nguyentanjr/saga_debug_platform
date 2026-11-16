package com.example.correlation.service;

import com.example.correlation.domain.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OutOfOrderPartitionTest {
    private static final Instant T0 = Instant.parse("2026-11-02T08:00:00Z");
    private static final ExecutionKey RUN = ExecutionKey.of(ExecutionKey.DEFAULT_SOURCE, "run-1");

    @Test
    void eventsFromDifferentTopicsReadBackwardsAreNoFinding() {
        ReplayPipeline p = new ReplayPipeline(null);
        deliver(p, "e2", "PAYMENT_COMPLETED", 10, "payments-0-7");
        deliver(p, "e1", "ORDER_CREATED", 0, "orders-0-3");

        assertThat(p.outcome(RUN.canonical()).anomalies).doesNotContain("OUT_OF_ORDER");
    }

    @Test
    void eventsFromOnePartitionReadBackwardsAreStillAFinding() {
        ReplayPipeline p = new ReplayPipeline(null);
        deliver(p, "e2", "PAYMENT_COMPLETED", 10, "orders-0-8");
        deliver(p, "e1", "ORDER_CREATED", 0, "orders-0-3");

        assertThat(p.outcome(RUN.canonical()).anomalies).contains("OUT_OF_ORDER");
    }

    @Test
    void twoPartitionsOfOneTopicAreNotOrderedEither() {
        ReplayPipeline p = new ReplayPipeline(null);
        deliver(p, "e2", "PAYMENT_COMPLETED", 10, "orders-1-8");
        deliver(p, "e1", "ORDER_CREATED", 0, "orders-0-3");

        assertThat(p.outcome(RUN.canonical()).anomalies).doesNotContain("OUT_OF_ORDER");
    }

    @Test
    void aTopicNameWithDashesKeepsItsPartition() {
        assertThat(AnomalyDetectionService.partitionOf("CORE-TRANSFER-TRANSACTION-RESULT-4-587857"))
                .isEqualTo("CORE-TRANSFER-TRANSACTION-RESULT-4");
        assertThat(AnomalyDetectionService.partitionOf(null)).isEmpty();
    }

    private static void deliver(ReplayPipeline p, String id, String type, long afterSeconds, String transportRef) {
        Instant at = T0.plusSeconds(afterSeconds);
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity(ExecutionKey.DEFAULT_SOURCE, RUN, id, null, CorrelationBasis.DECLARED_KEY),
                "ORDER_SAGA", type, null, "svc", at, "{}", Instant.now(), IngestedEvent.Causality.NONE, transportRef)));
    }
}
