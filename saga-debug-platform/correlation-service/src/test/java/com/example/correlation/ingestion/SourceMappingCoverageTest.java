package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SourceMappingCoverageTest {
    private static final Instant TRANSPORT_TIME = Instant.parse("2026-01-01T12:00:00Z");

    private final CorrelationService correlationService = mock(CorrelationService.class);

    @Test
    void aFieldIsFoundWhicheverOfItsDeclaredPlacesThisEventUsedIt() {
        String mapping = """
                name: versioned
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: [orderId, data.orderId]
                """;

        assertThat(ingest(mapping, "orders",
                """
                {"id":"e1","orderId":"ORD-1"}""").executionKey().rawId()).isEqualTo("ORD-1");

        assertThat(ingest(mapping, "orders",
                """
                {"id":"e2","data":{"orderId":"ORD-2"}}""").executionKey().rawId()).isEqualTo("ORD-2");
    }

    @Test
    void theFirstDeclaredPlaceThatHasAValueWins() {
        IngestedEvent ingested = ingest("""
                name: versioned
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: [orderId, data.orderId]
                """, "orders", """
                {"id":"e3","orderId":"TOP","data":{"orderId":"NESTED"}}""");

        assertThat(ingested.executionKey().rawId()).isEqualTo("TOP");
    }

    @Test
    void aBodyEmbeddedAsAJsonStringIsRead() {
        IngestedEvent ingested = ingest("""
                name: outbox
                topicPattern: 'outbox\\.events'
                fields:
                  eventId: id
                  executionId: payload.orderId
                  eventType: payload.status
                """, "outbox.events", """
                {"id":"e4","payload":"{\\"orderId\\":\\"ORD-9\\",\\"status\\":\\"SHIPPED\\"}"}""");

        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-9");
        assertThat(ingested.eventType()).isEqualTo("SHIPPED");
    }

    @Test
    void aStringFieldReadDirectlyIsNotReinterpretedAsADocument() {
        IngestedEvent ingested = ingest("""
                name: outbox
                topicPattern: 'outbox\\.events'
                fields:
                  eventId: id
                  executionId: note
                """, "outbox.events", """
                {"id":"e5","note":"{not really json"}""");

        assertThat(ingested.executionKey().rawId()).isEqualTo("{not really json");
    }

    @Test
    void anEventTypeCarriedOnlyByTheTopicNameIsReadAndTranslated() {
        IngestedEvent ingested = ingest("""
                name: topic-per-type
                topicPattern: 'shop\\..*'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: source
                eventTypes:
                  "shop.order.placed": ORDER_CREATED
                """, "shop.order.placed", """
                {"id":"e6","orderId":"ORD-3"}""");

        assertThat(ingested.eventType()).isEqualTo("ORDER_CREATED");
    }

    @Test
    void theUndeclaredFallbackToTheSourceNameIsTranslatedToo() {
        IngestedEvent ingested = ingest("""
                name: topic-per-type
                topicPattern: 'shop\\..*'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: null
                eventTypes:
                  "shop.order.placed": ORDER_CREATED
                """, "shop.order.placed", """
                {"id":"e7","orderId":"ORD-4"}""");

        assertThat(ingested.eventType()).isEqualTo("ORDER_CREATED");
    }

    @Test
    void aVersionedEventNameIsTranslatedByPattern() {
        String mapping = """
                name: versioned-types
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: type
                eventTypes:
                  "order\\\\.created(\\\\.v[0-9]+)?": ORDER_CREATED
                """;

        assertThat(ingest(mapping, "orders", """
                {"id":"e8","orderId":"O","type":"order.created"}""").eventType())
                .isEqualTo("ORDER_CREATED");
        assertThat(ingest(mapping, "orders", """
                {"id":"e9","orderId":"O","type":"order.created.v3"}""").eventType())
                .isEqualTo("ORDER_CREATED");
    }

    @Test
    void anExactMatchWinsOverAPatternThatWouldAlsoMatch() {
        IngestedEvent ingested = ingest("""
                name: versioned-types
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: type
                eventTypes:
                  "order.created.v2": ORDER_CREATED_V2
                  "order\\\\.created.*": ORDER_CREATED
                """, "orders", """
                {"id":"e10","orderId":"O","type":"order.created.v2"}""");

        assertThat(ingested.eventType()).isEqualTo("ORDER_CREATED_V2");
    }

    @Test
    void theEventTimeIsReadFromAHeader() {
        IngestedEvent ingested = ingest("""
                name: header-clock
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventTime: header:occurred-at
                """, "orders", """
                {"id":"e11","orderId":"O"}""",
                Map.of("occurred-at", "2026-01-01T09:30:00Z"));

        assertThat(ingested.eventTime()).isEqualTo(Instant.parse("2026-01-01T09:30:00Z"));
    }

    @Test
    void anEpochEventTimeIsReadFromAHeader() {
        IngestedEvent ingested = ingest("""
                name: header-clock
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventTime: header:ts
                """, "orders", """
                {"id":"e12","orderId":"O"}""",
                Map.of("ts", "1767258000000"));

        assertThat(ingested.eventTime()).isEqualTo(Instant.ofEpochMilli(1767258000000L));
    }

    @Test
    void theBusinessKeyIsReadFromWhereTheSourceKeepsIt() {
        IngestedEvent ingested = ingest("""
                name: hotel
                topicPattern: 'bookings'
                fields:
                  eventId: id
                  executionId: sagaRef
                  businessKey: reservation.reference
                """, "bookings", """
                {"id":"e13","sagaRef":"S-1","reservation":{"reference":"BK-2026-001"}}""");

        assertThat(ingested.businessKey()).isEqualTo("BK-2026-001");
    }

    @Test
    void aSecondIdentifierIsReadFromWhereTheSourceDeclaresIt() {
        SourceMapping mapping = parse("""
                name: split-ids
                topicPattern: 'payments'
                fields:
                  eventId: id
                  executionId: paymentId
                  aliasKey: context.orderNumber
                """);

        RawObservation observation = new RawObservation("payments", "{}", TRANSPORT_TIME,
                null, null, Map.of());
        com.fasterxml.jackson.databind.JsonNode payload = payload("""
                {"id":"e15","paymentId":"PAY-7","context":{"orderNumber":"ORD-7"}}""");

        assertThat(mapping.executionId(payload, observation)).contains("PAY-7");
        assertThat(mapping.aliasKey(payload, observation)).contains("ORD-7");
    }

    @Test
    void aSourceThatDeclaresNoSecondIdentifierHasNone() {
        SourceMapping mapping = parse("""
                name: single-id
                topicPattern: 'payments'
                fields:
                  executionId: paymentId
                """);

        assertThat(mapping.aliasKey(payload("""
                {"paymentId":"PAY-7","orderId":"ORD-7"}"""), null)).isEmpty();
    }

    @Test
    void anObservationMatchingNoneOfTheDeclaredPlacesIsStillRefused() {
        ingestRaw("""
                name: versioned
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: [orderId, data.orderId]
                """, "orders", """
                {"id":"e14","somethingElse":"X"}""", Map.of());

        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    private IngestedEvent ingest(String mappingYaml, String sourceRef, String payload) {
        return ingest(mappingYaml, sourceRef, payload, Map.of());
    }

    private IngestedEvent ingest(String mappingYaml, String sourceRef, String payload,
            Map<String, String> headers) {
        ingestRaw(mappingYaml, sourceRef, payload, headers);

        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }

    private void ingestRaw(String mappingYaml, String sourceRef, String payload,
            Map<String, String> headers) {
        org.mockito.Mockito.reset(correlationService);

        SourceMapping mapping = parse(mappingYaml);

        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mapping.matches(topic) ? mapping : SourceMapping.identity();
            }
        };

        new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry,
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager())
                .ingest(new RawObservation(sourceRef, payload, TRANSPORT_TIME,
                        "fallback-id", null, headers));
    }

    private static com.fasterxml.jackson.databind.JsonNode payload(String json) {
        try {
            return new ObjectMapper().readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test payload", e);
        }
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
