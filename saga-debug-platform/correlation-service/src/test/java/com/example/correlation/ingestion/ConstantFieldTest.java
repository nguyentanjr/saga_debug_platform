package com.example.correlation.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ConstantFieldTest {
    private static SourceMapping mapping(String serviceName) throws Exception {
        return new ObjectMapper(new YAMLFactory()).readValue("""
                name: hotel-replies
                topicPattern: 'room-booking\\.outbox\\.events'
                fields:
                  executionId: key
                  eventType: status
                  serviceName: %s
                """.formatted(serviceName), SourceMapping.class);
    }

    private static JsonNode body(String json) throws Exception {
        return new ObjectMapper().readTree(json);
    }

    @Test
    void aConstantNamesTheServiceTheSourceDoesNotCarry() throws Exception {
        RawObservation raw = RawObservation.of("room-booking.outbox.events", "{\"status\":\"BOOKED\"}", Instant.EPOCH);

        assertThat(mapping("const:hotel-service").serviceName(body(raw.payload()), raw)).contains("hotel-service");
    }

    @Test
    void aConstantIsOnlyAFallbackWhenListedAfterAPathThatResolves() throws Exception {
        RawObservation raw = RawObservation.of("room-booking.outbox.events",
                "{\"status\":\"BOOKED\",\"service\":\"hotel-v2\"}", Instant.EPOCH);

        assertThat(mapping("[service, 'const:hotel-service']").serviceName(body(raw.payload()), raw)).contains("hotel-v2");
    }

    @Test
    void anEmptyConstantResolvesToNothing() throws Exception {
        RawObservation raw = RawObservation.of("room-booking.outbox.events", "{\"status\":\"BOOKED\"}", Instant.EPOCH);

        assertThat(mapping("'const:'").serviceName(body(raw.payload()), raw)).isEmpty();
    }
}
