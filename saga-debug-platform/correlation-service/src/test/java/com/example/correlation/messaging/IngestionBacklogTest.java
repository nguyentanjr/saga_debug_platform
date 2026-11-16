package com.example.correlation.messaging;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

class IngestionBacklogTest {
    @Test
    void theBacklogIsEndMinusCommittedNotWhereTheConsumerHasFetchedTo() {
        TopicPartition held = new TopicPartition("order-created", 0);
        TopicPartition caughtUp = new TopicPartition("order-created", 1);
        TopicPartition neverCommitted = new TopicPartition("payment-completed", 0);

        Map<TopicPartition, Long> committed = Map.of(held, 29L, caughtUp, 16L);
        Map<TopicPartition, Long> ends = Map.of(held, 49L, caughtUp, 16L, neverCommitted, 7L);

        assertThat(KafkaClusterListeners.backlogOf(committed, ends)).isEqualTo(20L);
    }

    @Test
    void nothingFetchedYetIsNotReportedAsCaughtUp() {
        assertThat(KafkaClusterListeners.backlogOf(Map.of(), Map.of())).isNull();
        assertThat(new IngestionBacklogHealth(() -> null, 100).health().getStatus()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void pastTheThresholdTheComponentSaysLaggingWithoutGoingDown() {
        assertThat(new IngestionBacklogHealth(() -> 100L, 100).health().getStatus()).isEqualTo(Status.UP);
        var lagging = new IngestionBacklogHealth(() -> 101L, 100).health();
        assertThat(lagging.getStatus()).isEqualTo(IngestionBacklogHealth.LAGGING);
        assertThat(lagging.getDetails()).containsEntry("backlog", 101L).containsEntry("threshold", 100L);
    }
}
