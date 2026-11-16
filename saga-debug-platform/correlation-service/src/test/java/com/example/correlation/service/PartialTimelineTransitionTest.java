package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PartialTimelineTransitionTest {
    private static final ExecutionKey RUN = ExecutionKey.of("retest-replay-9", "4a6e4dad");

    private static final List<String[]> CAPTURED = List.of(
            new String[] {"t_order-0-6", "ORDER_CREATED", "2026-10-11T16:24:17.679Z"},
            new String[] {"t_stock-0-3", "STOCK_REJECTED", "2026-10-11T16:24:17.691Z"},
            new String[] {"t_order-0-7", "ORDER_VERDICT_BROADCAST", "2026-10-11T16:24:18.222Z"},
            new String[] {"t_payment-0-3", "PAYMENT_REJECTED", "2026-10-11T16:24:18.222Z"});

    @Test
    void reverseDeliveryEndsWithTheSameTransitionFindingsAsForward() {
        List<String[]> reversed = new ArrayList<>(CAPTURED);
        Collections.reverse(reversed);

        assertThat(standingTransitions(reversed)).isEqualTo(standingTransitions(CAPTURED));
    }

    private static List<String> standingTransitions(List<String[]> deliveryOrder) {
        ReplayPipeline p = new ReplayPipeline(kmsAsCaptured());
        for (String[] e : deliveryOrder) {
            Instant at = Instant.parse(e[2]);
            p.deliver(List.of(new IngestedEvent(
                    new IngestedEvent.Identity("retest-replay-9", RUN, e[0], null, CorrelationBasis.DECLARED_KEY),
                    "KMS_ORDER_SAGA", e[1], null, "svc", at, "{}", at, IngestedEvent.Causality.NONE, null)));
        }
        return p.anomalies.stream()
                .filter(a -> a.getAnomalyType() == AnomalyType.INVALID_TRANSITION && a.getRetractedAt() == null)
                .map(Anomaly::getObservedValue)
                .toList();
    }

    private static WorkflowRegistry kmsAsCaptured() {
        WorkflowRegistry workflows = new WorkflowRegistry();
        Path dir = Path.of("src/test/resources/ext-i9").toAbsolutePath();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", dir.toString());
        workflows.load();
        return workflows;
    }
}
