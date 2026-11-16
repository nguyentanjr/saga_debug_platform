package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.messaging.SagaEventConsumer;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ForeignSystemIngestionTest {
    private static final String TOPIC = "legacy.orders.events";
    private static final Instant EVENT_TIME = Instant.parse("2026-01-01T00:00:04Z");

    private CorrelationService correlationService;
    private SagaEventConsumer consumer;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);

        SourceMappingRegistry registry = new SourceMappingRegistry();
        registry.load();

        consumer = new SagaEventConsumer(new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new com.example.correlation.ingestion.SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry,
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager()));
    }

    @Test
    void theMappingForTheForeignSystemIsDiscovered() {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        registry.load();

        assertThat(registry.sourceNames()).contains("legacy-order-system");
        assertThat(registry.forTopic(TOPIC).getName()).isEqualTo("legacy-order-system");
    }

    @Test
    void aForeignPayloadIsReadWithoutChangingTheSourceSystem() {
        consumer.handleEvent(record("""
                {
                  "id": "evt-8891",
                  "type": "order.payment.captured",
                  "occurredAt": "2026-01-01T00:00:04Z",
                  "emittedBy": { "service": "billing-api" },
                  "context":   { "orderNumber": "ORD-42", "causedBy": "evt-8890" }
                }
                """));

        IngestedEvent ingested = captured();
        assertThat(ingested.eventId()).isEqualTo("evt-8891");

        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-42");

        assertThat(ingested.eventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(ingested.serviceName()).isEqualTo("billing-api");
        assertThat(ingested.eventTime()).isEqualTo(EVENT_TIME);
        assertThat(ingested.causationId()).isEqualTo("evt-8890");
    }

    @Test
    void theTraceIdIsReadFromWhereTheirSystemKeepsIt() {
        consumer.handleEvent(record("""
                {
                  "id": "evt-8891",
                  "type": "order.payment.captured",
                  "occurredAt": "2026-01-01T00:00:04Z",
                  "context": {
                    "orderNumber": "ORD-42",
                    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736"
                  }
                }
                """));

        assertThat(captured().traceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
    }

    @Test
    void aSourceThatReportsNoTraceYieldsNoTraceId() {
        consumer.handleEvent(record("""
                {
                  "id": "evt-8892",
                  "type": "order.created",
                  "occurredAt": "2026-01-01T00:00:04Z",
                  "context": { "orderNumber": "ORD-42" }
                }
                """));

        assertThat(captured().traceId()).isNull();
    }

    @Test
    void theEventTimeComesFromTheirFieldNotFromTheBroker() {
        consumer.handleEvent(record("""
                {
                  "id": "evt-1",
                  "type": "order.created",
                  "occurredAt": "2026-01-01T00:00:04Z",
                  "context": { "orderNumber": "ORD-42" }
                }
                """));

        assertThat(captured().eventTime()).isEqualTo(EVENT_TIME);
    }

    @Test
    void anUnmappedEventTypePassesThroughUnchanged() {
        consumer.handleEvent(record("""
                {
                  "id": "evt-9",
                  "type": "order.gift.wrapped",
                  "occurredAt": "2026-01-01T00:00:04Z",
                  "context": { "orderNumber": "ORD-42" }
                }
                """));

        assertThat(captured().eventType()).isEqualTo("order.gift.wrapped");
    }

    @Test
    void anEventWithoutTheDeclaredCorrelationKeyIsNotIngested() {
        consumer.handleEvent(record("""
                {
                  "id": "evt-7",
                  "type": "order.created",
                  "occurredAt": "2026-01-01T00:00:04Z"
                }
                """));

        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    @Test
    void aTopicWithNoMappingFallsBackToThePlatformsOwnEnvelope() {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        registry.load();

        SourceMapping fallback = registry.forTopic("payment-completed");
        assertThat(fallback.getName()).isEqualTo("default");
        assertThat(fallback.getFields().getExecutionId()).containsExactly("sagaId");
    }

    @Test
    void observingANewSystemIsTwoFilesAndNoJavaChange() {
        SourceMappingRegistry sources = new SourceMappingRegistry();
        sources.load();
        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();

        assertThat(sources.sourceNames()).contains("legacy-order-system");
        assertThat(workflows.workflowNames()).contains("ORDER_SAGA");
    }

    private IngestedEvent captured() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }

    private static ConsumerRecord<String, String> record(String json) {
        return new ConsumerRecord<>(TOPIC, 0, 0L,
                Instant.parse("2026-01-01T00:05:00Z").toEpochMilli(),
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, "key", json,
                new org.apache.kafka.common.header.internals.RecordHeaders(), null);
    }
}
