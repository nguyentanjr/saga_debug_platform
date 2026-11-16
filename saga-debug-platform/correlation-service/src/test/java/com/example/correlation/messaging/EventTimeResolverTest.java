package com.example.correlation.messaging;

import com.example.correlation.domain.EventTimeSource;
import com.example.saga.events.OrderCreatedEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventTimeResolverTest {
    private static final Instant TRANSPORT = Instant.parse("2026-01-01T00:00:30Z");
    private static final Instant EVENT = Instant.parse("2026-01-01T00:00:00Z");

    private final EventTimeResolver resolver = new EventTimeResolver();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void resolvesTheTimestampProducedByTheConfiguredSerializer() throws Exception {
        OrderCreatedEvent event = new OrderCreatedEvent();
        event.setOrderId(UUID.randomUUID());
        event.setSagaId(UUID.randomUUID());
        event.setCustomerId("customer-1");
        event.setProductId("PRODUCT-1");
        event.setQuantity(1);
        event.setAmount(BigDecimal.TEN);
        event.setTimestamp(EVENT);

        byte[] wire;
        try (JsonSerializer<Object> serializer = new JsonSerializer<>()) {
            wire = serializer.serialize("order-created", event);
        }
        JsonNode payload = mapper.readTree(wire);

        EventTimeResolver.Resolved resolved = resolver.resolve(payload, TRANSPORT);

        assertThat(resolved.source()).isEqualTo(EventTimeSource.DECLARED);
        assertThat(resolved.eventTime()).isEqualTo(EVENT);
        assertThat(resolved.eventTime()).isNotEqualTo(TRANSPORT);
    }

    @Test
    void readsIso8601Timestamps() throws Exception {
        assertResolvesTo("{\"timestamp\":\"2026-01-01T00:00:00Z\"}", EVENT);
    }

    @Test
    void readsDecimalEpochSecondsWithSubSecondPrecision() throws Exception {
        assertResolvesTo("{\"timestamp\":1767225600.250}", EVENT.plusMillis(250));
    }

    @Test
    void readsEpochMillis() throws Exception {
        assertResolvesTo("{\"timestamp\":1767225600000}", EVENT);
    }

    @Test
    void readsEpochSeconds() throws Exception {
        assertResolvesTo("{\"timestamp\":1767225600}", EVENT);
    }

    @Test
    void fallsBackToTransportTimeWhenTimestampIsAbsent() throws Exception {
        EventTimeResolver.Resolved resolved = resolve("{\"eventId\":\"e1\"}");

        assertThat(resolved.source()).isEqualTo(EventTimeSource.TRANSPORT);
        assertThat(resolved.eventTime()).isEqualTo(TRANSPORT);
        assertThat(resolved.isObserved()).isFalse();
    }

    @Test
    void fallsBackToTransportTimeWhenTimestampIsUnparseable() throws Exception {
        assertThat(resolve("{\"timestamp\":\"not-a-time\"}").source())
                .isEqualTo(EventTimeSource.TRANSPORT);
        assertThat(resolve("{\"timestamp\":null}").source())
                .isEqualTo(EventTimeSource.TRANSPORT);
        assertThat(resolve("{\"timestamp\":\"\"}").source())
                .isEqualTo(EventTimeSource.TRANSPORT);
    }

    @Test
    void fallsBackToTransportTimeWhenPayloadIsMissing() {
        assertThat(resolver.resolve(null, TRANSPORT).source())
                .isEqualTo(EventTimeSource.TRANSPORT);
    }

    private void assertResolvesTo(String json, Instant expected) throws Exception {
        EventTimeResolver.Resolved resolved = resolve(json);
        assertThat(resolved.source()).isEqualTo(EventTimeSource.DECLARED);
        assertThat(resolved.eventTime()).isEqualTo(expected);
    }

    private EventTimeResolver.Resolved resolve(String json) throws Exception {
        return resolver.resolve(mapper.readTree(json), TRANSPORT);
    }
}
