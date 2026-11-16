package com.example.correlation.ingestion;

import com.example.correlation.controller.IngestController;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.messaging.SagaEventConsumer;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

class TransportPortabilityTest {
    private static final String MAPPING = """
            name: outbox-system
            topicPattern: 'probe\\.outbox\\.events'
            fields:
              executionId: key
              eventId: header:id
              eventType: status
            eventTypes:
              "BOOKED": ROOM_BOOKED
            """;

    private static final String SOURCE = "probe.outbox.events";
    private static final String BODY = """
            {"status":"BOOKED"}""";
    private static final String SAGA_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6";

    private CorrelationService correlationService;
    private EventIngestionService ingestion;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);

        SourceMapping mapping = parse(MAPPING);
        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mapping.matches(topic) ? mapping : SourceMapping.identity();
            }
        };

        ingestion = new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry,
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager());
    }

    @Test
    void theRecordKeyIsReadOverKafka() {
        new SagaEventConsumer(ingestion).handleEvent(kafkaRecord());

        IngestedEvent ingested = captured();
        assertThat(ingested.executionKey().rawId()).isEqualTo(SAGA_ID);
        assertThat(ingested.eventId()).isEqualTo("evt-1");
        assertThat(ingested.eventType()).isEqualTo("ROOM_BOOKED");
    }

    @Test
    void theSameMappingReadsTheSameValuesOverHttp() {
        post(Map.of("id", "evt-1", IngestController.KEY_HEADER, SAGA_ID));

        IngestedEvent ingested = captured();
        assertThat(ingested.executionKey().rawId()).isEqualTo(SAGA_ID);
        assertThat(ingested.eventId()).isEqualTo("evt-1");
        assertThat(ingested.eventType()).isEqualTo("ROOM_BOOKED");
    }

    @Test
    void aHeaderIsFoundWhateverTheClientCapitalised() {
        post(Map.of("ID", "evt-1", "x-observation-key", SAGA_ID));

        assertThat(captured().eventId()).isEqualTo("evt-1");
    }

    @Test
    void anObservationWithNoKeyAnywhereIsStillRefused() {
        IngestionResult result = ingestOverHttp(Map.of("id", "evt-2"));

        assertThat(result.outcome()).isEqualTo(IngestionResult.Outcome.NOT_CORRELATABLE);
    }

    private void post(Map<String, String> headers) {
        assertThat(ingestOverHttp(headers).isAccepted()).isTrue();
    }

    private IngestionResult ingestOverHttp(Map<String, String> headers) {
        reset(correlationService);

        Map<String, String> lowercased = new java.util.LinkedHashMap<>();
        headers.forEach((name, value) ->
                lowercased.put(name.toLowerCase(java.util.Locale.ROOT), value));
        String key = lowercased.get(IngestController.KEY_HEADER.toLowerCase(java.util.Locale.ROOT));

        return ingestion.ingest(new RawObservation(
                SOURCE, BODY, Instant.parse("2026-01-01T00:00:00Z"), null, key, lowercased));
    }

    private ConsumerRecord<String, String> kafkaRecord() {
        reset(correlationService);

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("id", "evt-1".getBytes(StandardCharsets.UTF_8)));

        return new ConsumerRecord<>(SOURCE, 0, 0L, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, 0, 0, SAGA_ID, BODY, headers, Optional.empty());
    }

    private IngestedEvent captured() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
