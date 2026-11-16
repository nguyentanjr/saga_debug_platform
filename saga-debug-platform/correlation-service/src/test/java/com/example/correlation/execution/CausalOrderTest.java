package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CausalOrderTest {
    private static final Instant T = Instant.parse("2026-10-03T19:33:21Z");
    private WorkflowSpec workflow;

    @BeforeEach
    void load() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        workflow = registry.byType("ORDER_SAGA").orElseThrow();
    }

    private static List<SagaEvent> measuredRun() {
        return List.of(
                event("order", "ORDER_CREATED", null, 1517),
                event("payment", "PAYMENT_COMPLETED", "order", 1559),
                event("inventory", "INVENTORY_RESERVED", "payment", 787));
    }

    @Test
    void aReservationStampedBeforeItsCauseStillCompletesTheRun() {
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, measuredRun());

        assertThat(timeline.status()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(timeline.postTerminal()).isEmpty();
        assertThat(timeline.regression()).isEmpty();
        assertThat(timeline.inBusinessOrder()).extracting(SagaEvent::getEventId)
                .containsExactly("order", "payment", "inventory");
    }

    @Test
    void theAnswerDoesNotDependOnArrivalOrder() {
        List<SagaEvent> shuffled = new ArrayList<>(measuredRun());
        for (int i = 0; i < 6; i++) {
            Collections.rotate(shuffled, 1);
            if (i % 2 == 1) Collections.swap(shuffled, 0, 1);
            EvidenceTimeline timeline = EvidenceTimeline.of(workflow, shuffled);
            assertThat(timeline.status()).isEqualTo(SagaStatus.COMPLETED);
            assertThat(timeline.postTerminal()).isEmpty();
        }
    }

    @Test
    void theRunEndsNoEarlierThanTheCauseOfItsEnding() {
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, measuredRun());

        Instant start = timeline.startedAt().orElseThrow();
        Instant end = timeline.endedAt().orElseThrow();
        assertThat(start).isEqualTo(T.plusMillis(1517));
        assertThat(end).isAfterOrEqualTo(start);
    }

    @Test
    void withoutCausationTheClockStillDecides() {
        List<SagaEvent> unlinked = List.of(
                event("order", "ORDER_CREATED", null, 1517),
                event("payment", "PAYMENT_COMPLETED", null, 1559),
                event("inventory", "INVENTORY_RESERVED", null, 787));

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, unlinked);

        assertThat(timeline.inBusinessOrder()).extracting(SagaEvent::getEventId)
                .containsExactly("inventory", "order", "payment");
    }

    @Test
    void causationDoesNotHideAnEventThatReallyCameAfterTheEnding() {
        List<SagaEvent> events = List.of(
                event("order", "ORDER_CREATED", null, 0),
                event("payment", "PAYMENT_COMPLETED", "order", 100),
                event("inventory", "INVENTORY_RESERVED", "payment", 200),
                event("late", "PAYMENT_COMPLETED", "order", 900));

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);

        assertThat(timeline.postTerminal()).isPresent();
        assertThat(timeline.postTerminal().get().late().getEventId()).isEqualTo("late");
    }

    @Test
    void danglingOrSelfCausationIsIgnored() {
        List<SagaEvent> events = List.of(
                event("order", "ORDER_CREATED", "order", 1517),
                event("payment", "PAYMENT_COMPLETED", "elsewhere", 1559),
                event("inventory", "INVENTORY_RESERVED", null, 1600));

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);

        assertThat(timeline.status()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(timeline.inBusinessOrder()).extracting(SagaEvent::getEventId)
                .containsExactly("order", "payment", "inventory");
    }

    private static SagaEvent event(String id, String type, String causedBy, long millis) {
        SagaEvent e = new SagaEvent();
        e.setSagaId("run");
        e.setEventId(id);
        e.setEventType(type);
        e.setCausationId(causedBy);
        e.setTimestamp(T.plusMillis(millis));
        e.setReceivedAt(T.plusMillis(millis));
        return e;
    }
}
