package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AwaitedStepStuckTimeoutTest {
    private static final ExecutionKey RUN = ExecutionKey.of("ewallet", "TX-1");

    @Test
    void twoMinutesIntoAFifteenMinuteReleaseIsNotStuck() {
        ReplayPipeline p = heldThenCommission(120);

        p.detector.detectStuckSagas();

        assertThat(standing(p)).doesNotContain(AnomalyType.SAGA_STUCK);
    }

    @Test
    void silenceBeyondTheReleaseTimeoutIsStillStuck() {
        ReplayPipeline p = heldThenCommission(16 * 60);

        p.detector.detectStuckSagas();

        assertThat(standing(p)).contains(AnomalyType.SAGA_STUCK);
        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK)
                .singleElement()
                .satisfies(a -> assertThat(a.getThresholdSource()).isEqualTo("workflow:RELEASE_MONEY.timeout"));
    }

    private static ReplayPipeline heldThenCommission(int secondsAgo) {
        ReplayPipeline p = new ReplayPipeline(w2n());
        Instant at = Instant.now().minusSeconds(secondsAgo);
        deliver(p, "e1", "HOLD_SUCCEEDED", at);
        deliver(p, "e2", "COMMISSION_REQUESTED", at.plusMillis(40));
        return p;
    }

    private static List<AnomalyType> standing(ReplayPipeline p) {
        return p.anomalies.stream().filter(a -> a.getRetractedAt() == null).map(Anomaly::getAnomalyType).toList();
    }

    private static void deliver(ReplayPipeline p, String id, String type, Instant at) {
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
