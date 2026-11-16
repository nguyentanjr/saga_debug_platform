package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UnplacedEventAfterEndingTest {
    private static final ExecutionKey RUN = ExecutionKey.of("ewallet", "TX-2");
    private static final Instant T0 = Instant.parse("2026-10-15T09:00:00Z");

    @Test
    void anUnplacedEventAfterTheEndingIsNotAFinding() {
        ReplayPipeline p = compensated();
        deliver(p, "e5", "W2N_Holding_Success", 400);

        assertThat(standing(p)).doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void aPlacedEventAfterTheEndingStillIs() {
        ReplayPipeline p = compensated();
        deliver(p, "e5", "RELEASE_SUCCEEDED", 400);

        assertThat(standing(p)).contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    private static ReplayPipeline compensated() {
        ReplayPipeline p = new ReplayPipeline(w2n());
        deliver(p, "e1", "HOLD_SUCCEEDED", 0);
        deliver(p, "e2", "RELEASE_FAILED", 60);
        deliver(p, "e3", "UNHOLD_SUCCEEDED", 360);
        return p;
    }

    private static List<AnomalyType> standing(ReplayPipeline p) {
        return p.anomalies.stream().filter(a -> a.getRetractedAt() == null).map(Anomaly::getAnomalyType).toList();
    }

    private static void deliver(ReplayPipeline p, String id, String type, int second) {
        Instant at = T0.plusSeconds(second);
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity("ewallet", RUN, id, null, CorrelationBasis.DECLARED_KEY),
                "EWALLET_W2N_TRANSFER", type, null, "svc", at, "{}", at, IngestedEvent.Causality.NONE, null)));
    }

    private static WorkflowRegistry w2n() {
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory",
                Path.of("src/test/resources/ext-ewallet").toAbsolutePath().toString());
        workflows.load();
        return workflows;
    }
}
