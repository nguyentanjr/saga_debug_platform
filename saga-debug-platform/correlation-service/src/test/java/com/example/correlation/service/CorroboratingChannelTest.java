package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CorroboratingChannelTest {
    private static final ExecutionKey RUN = ExecutionKey.of("ewallet-core-transfer", "TX-3");
    private static final Instant T0 = Instant.parse("2026-10-15T10:00:00Z");

    @Test
    void theEndingConfirmedByAnotherTopicIsNotAFinding() {
        ReplayPipeline p = compensated("staging-ewallet-transfer-business-v2-0-812");
        deliver(p, "e4", "UNHOLD_SUCCEEDED", 362, "CORE-TRANSFER-TRANSACTION-RESULT-5-40417");

        assertThat(standing(p)).doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void theEndingRepeatedOnTheSameTopicIsStillAFinding() {
        ReplayPipeline p = compensated("staging-ewallet-transfer-business-v2-0-812");
        deliver(p, "e4", "UNHOLD_SUCCEEDED", 362, "staging-ewallet-transfer-business-v2-0-815");

        assertThat(standing(p)).contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void withoutATransportPositionNothingCanBeToldApart() {
        ReplayPipeline p = compensated(null);
        deliver(p, "e4", "UNHOLD_SUCCEEDED", 362, null);

        assertThat(standing(p)).contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void aDifferentEventAfterTheEndingIsStillAFinding() {
        ReplayPipeline p = compensated("staging-ewallet-transfer-business-v2-0-812");
        deliver(p, "e4", "RELEASE_SUCCEEDED", 362, "CORE-TRANSFER-TRANSACTION-RESULT-5-40417");

        assertThat(standing(p)).contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    private static ReplayPipeline compensated(String unholdPosition) {
        ReplayPipeline p = new ReplayPipeline(w2n());
        deliver(p, "e1", "HOLD_SUCCEEDED", 0, "staging-ewallet-transfer-management-v2-1-77");
        deliver(p, "e2", "RELEASE_FAILED", 60, "staging-ewallet-transfer-management-v2-1-91");
        deliver(p, "e3", "UNHOLD_SUCCEEDED", 360, unholdPosition);
        return p;
    }

    private static List<AnomalyType> standing(ReplayPipeline p) {
        return p.anomalies.stream().filter(a -> a.getRetractedAt() == null).map(Anomaly::getAnomalyType).toList();
    }

    private static void deliver(ReplayPipeline p, String id, String type, int second, String position) {
        Instant at = T0.plusSeconds(second);
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity("ewallet-core-transfer", RUN, id, null, CorrelationBasis.DECLARED_KEY),
                "EWALLET_W2N_TRANSFER", type, null, "svc", at, "{}", at, IngestedEvent.Causality.NONE, position)));
    }

    private static WorkflowRegistry w2n() {
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory",
                Path.of("src/test/resources/ext-ewallet").toAbsolutePath().toString());
        workflows.load();
        return workflows;
    }
}
