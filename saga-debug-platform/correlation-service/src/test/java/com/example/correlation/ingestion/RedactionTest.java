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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RedactionTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void declaredFieldsAreReplacedTopLevelAndNested() throws Exception {
        SourceMapping mapping = parse("""
                name: shop
                topicPattern: 'shop'
                redact: [creditCardNo, card.number]
                """);
        String raw = """
                {"sagaId":"S-1","creditCardNo":"4111111111111111","card":{"number":"5500","exp":"12/30"}}""";

        String stored = mapping.redact(raw);

        var tree = JSON.readTree(stored);
        assertThat(tree.path("creditCardNo").asText()).isEqualTo(SourceMapping.REDACTED);
        assertThat(tree.at("/card/number").asText()).isEqualTo(SourceMapping.REDACTED);
        assertThat(tree.at("/card/exp").asText()).isEqualTo("12/30");
        assertThat(tree.path("sagaId").asText()).isEqualTo("S-1");
        assertThat(stored).doesNotContain("4111111111111111").doesNotContain("5500");
    }

    @Test
    void aPayloadWithNothingToRedactIsKeptExactly() {
        String raw = "{\"sagaId\": \"S-1\",   \"amount\": 100.50}";

        assertThat(parse("name: a\ntopicPattern: 'a'\n").redact(raw)).isEqualTo(raw);
        assertThat(parse("name: a\ntopicPattern: 'a'\nredact: [creditCardNo]\n").redact(raw)).isEqualTo(raw);
    }

    @Test
    void aPayloadThatIsNotAnObjectIsKept() {
        SourceMapping mapping = parse("name: a\ntopicPattern: 'a'\nredact: [creditCardNo]\n");

        assertThat(mapping.redact("not json")).isEqualTo("not json");
        assertThat(mapping.redact("[1,2]")).isEqualTo("[1,2]");
    }

    @Test
    void theHotelCommandTopicsNeverStoreTheCardNumber() {
        CorrelationService correlation = mock(CorrelationService.class);
        EventIngestionService ingestion = new EventIngestionService(correlation, JSON,
                new SourceCompletenessCheck(), new DeduplicationCache(), new EventTimeResolver(),
                OutboxPatternIngestionTest.declaredMappings(), new PassthroughAliasResolver(),
                new InlineTransactionManager());

        ingestion.ingest(new RawObservation("payment.inbox.events", """
                {"type":"REQUEST","reservationId":"R-1","paymentDue":100,"creditCardNo":"4111111111111111"}""",
                Instant.parse("2026-01-01T00:00:00Z"), null, "SAGA-1", Map.of("id", "cmd-1")));

        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlation).processEvent(captor.capture());
        IngestedEvent ingested = captor.getValue();
        assertThat(ingested.payload()).doesNotContain("4111111111111111").contains("\"paymentDue\":100");
        assertThat(ingested.eventType()).isEqualTo("TAKE_PAYMENT_REQUESTED");
        assertThat(ingested.sagaId()).isEqualTo("hotel-saga::SAGA-1");
    }

    @Test
    void anUnreadablePayloadIsNotQuotedInTheRejection() {
        EventIngestionService ingestion = new EventIngestionService(mock(CorrelationService.class), JSON,
                new SourceCompletenessCheck(), new DeduplicationCache(), new EventTimeResolver(),
                OutboxPatternIngestionTest.declaredMappings(), new PassthroughAliasResolver(),
                new InlineTransactionManager());

        IngestionResult result = ingestion.ingest(new RawObservation("payment.inbox.events",
                "{\"type\":\"REQUEST\",\"creditCardNo\":\"4111111111111111\",",
                Instant.parse("2026-01-01T00:00:00Z"), null, "SAGA-1", Map.of("id", "cmd-2")));

        assertThat(result.outcome()).isEqualTo(IngestionResult.Outcome.UNREADABLE);
        assertThat(result.reason()).doesNotContain("4111111111111111");
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
