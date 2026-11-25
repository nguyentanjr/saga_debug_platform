package com.example.correlation.messaging;

import com.example.correlation.ingestion.EventIngestionService;
import com.example.correlation.ingestion.IngestionResult;
import com.example.correlation.ingestion.RawObservation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class SagaEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(SagaEventConsumer.class);

    private final EventIngestionService ingestionService;

    public SagaEventConsumer(EventIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    public void handle(ConsumerRecord<String, String> record, String origin) {
        String transportRef = record.topic() + "-" + record.partition() + "-" + record.offset();
        RawObservation raw = new RawObservation(
                record.topic(),
                record.value(),
                Instant.ofEpochMilli(record.timestamp()),
                transportRef,
                record.key(),
                headersOf(record),
                origin);

        IngestionResult result = ingestionService.ingest(raw);

        if (result.isPermanentlyRejected()) {
            log.error("Dropping record cluster={} topic={} partition={} offset={}: {} ({})",
                    origin != null ? origin : "default", record.topic(), record.partition(),
                    record.offset(), result.outcome(), result.reason());
        }
    }

    public void handleEvent(ConsumerRecord<String, String> record) {
        handle(record, null);
    }

    private static Map<String, String> headersOf(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            if (header.key() == null || header.value() == null) continue;
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }
}
