package com.example.correlation.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.EvidenceTimeline;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

class KmsVerdictDeclarationTest {
    private static final Instant T0 = Instant.parse("2026-10-11T23:20:00Z");
    private static WorkflowSpec spec;
    private static Map<String, String> verdicts;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() throws Exception {
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", declarations().toString());
        workflows.load();
        spec = workflows.byType("KMS_ORDER_SAGA").orElseThrow();

        File mapping = declarations().resolve("ingestion/kms-order-request.yaml").toFile();
        Map<String, Object> source = (Map<String, Object>) new ObjectMapper(new YAMLFactory())
                .readValue(mapping, Map.class).get("source");
        verdicts = (Map<String, String>) source.get("eventTypes");
    }

    @Test
    void bothArmsSucceededCompletes() {
        assertEnds(SagaStatus.COMPLETED, "STOCK_RESERVED", "PAYMENT_ACCEPTED", verdicts.get("Success"));
    }

    @Test
    void stockFailedWhilePaymentSucceededFails() {
        assertEnds(SagaStatus.FAILED, "STOCK_REJECTED", "PAYMENT_ACCEPTED", verdicts.get("Failed"));
    }

    @Test
    void bothArmsFailedFails() {
        assertEnds(SagaStatus.FAILED, "STOCK_REJECTED", "PAYMENT_REJECTED", verdicts.get("Failed"));
    }

    @Test
    void paymentFailedWhileStockSucceededAwaitsTheSilentRollback() {
        assertEnds(SagaStatus.COMPENSATING, "STOCK_RESERVED", "PAYMENT_REJECTED", verdicts.get("Rollback"));
    }

    @Test
    void aRejectedArmDoesNotEndTheRunBeforeTheJoinHasSpoken() {
        EvidenceTimeline timeline = EvidenceTimeline.of(spec, events("STOCK_REJECTED"));
        assertThat(timeline.status()).isNotIn(SagaStatus.FAILED, SagaStatus.COMPLETED);
    }

    private static void assertEnds(SagaStatus expected, String stock, String payment, String verdict) {
        EvidenceTimeline timeline = EvidenceTimeline.of(spec, events(stock, payment, verdict));
        assertThat(timeline.status()).isEqualTo(expected);
        assertThat(timeline.contestedOutcome()).isEmpty();
        assertThat(timeline.postTerminal()).isEmpty();
        assertThat(timeline.regression()).isEmpty();
    }

    private static List<SagaEvent> events(String... afterRequest) {
        List<String> types = new ArrayList<>();
        types.add("ORDER_CREATED");
        types.addAll(List.of(afterRequest));
        List<SagaEvent> events = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            SagaEvent event = new SagaEvent();
            event.setEventId("t_order-0-" + i);
            event.setSagaId("kms::ORD-1");
            event.setEventType(types.get(i));
            event.setServiceName("test");
            event.setTimestamp(T0.plusSeconds(i));
            event.setReceivedAt(T0.plusSeconds(i));
            event.setPayload("{}");
            events.add(event);
        }
        return events;
    }

    private static Path declarations() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        return root.resolve("declarations");
    }
}
