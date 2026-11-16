package com.example.correlation.messaging;

import com.example.correlation.domain.EventTimeSource;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;

@Component
public class EventTimeResolver {
    private static final Logger log = LoggerFactory.getLogger(EventTimeResolver.class);

    private static final long EPOCH_MILLIS_THRESHOLD = 100_000_000_000L;

    public record Resolved(Instant eventTime, EventTimeSource source) {
        public boolean isObserved() {
            return source == EventTimeSource.DECLARED;
        }
    }

    public Resolved resolve(JsonNode payload, Instant transportTime) {
        return resolve(payload, transportTime, "timestamp");
    }

    public Resolved resolveValue(String value, Instant transportTime) {
        Instant parsed = parseText(value);
        return parsed != null
                ? new Resolved(parsed, EventTimeSource.DECLARED)
                : new Resolved(transportTime, EventTimeSource.TRANSPORT);
    }

    public Resolved resolve(JsonNode payload, Instant transportTime, String path) {
        Instant fromPayload = readTimestamp(payload, path);
        if (fromPayload != null) {
            return new Resolved(fromPayload, EventTimeSource.DECLARED);
        }
        return new Resolved(transportTime, EventTimeSource.TRANSPORT);
    }

    private Instant readTimestamp(JsonNode payload, String path) {
        if (payload == null || path == null || path.isBlank()) {
            return null;
        }
        JsonNode node = payload;
        for (String segment : path.split("\\.")) {
            if (node == null || !node.has(segment)) {
                return null;
            }
            node = node.get(segment);
        }
        if (node == null || node.isNull()) {
            return null;
        }

        if (node.isFloatingPointNumber()) {
            return Instant.ofEpochMilli(Math.round(node.asDouble() * 1000d));
        }

        if (node.isNumber()) {
            return fromEpoch(node.asLong());
        }

        if (node.isTextual()) {
            return parseText(node.asText());
        }

        return null;
    }

    private Instant parseText(String value) {
        String text = value == null ? "" : value.trim();
        if (text.isEmpty()) {
            return null;
        }
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            try {
                double epochSeconds = Double.parseDouble(text);
                return text.contains(".")
                        ? Instant.ofEpochMilli(Math.round(epochSeconds * 1000d))
                        : fromEpoch((long) epochSeconds);
            } catch (NumberFormatException ignored) {
                log.warn("Unparseable event timestamp '{}'; falling back to transport time", text);
                return null;
            }
        }
    }

    private Instant fromEpoch(long value) {
        return value < EPOCH_MILLIS_THRESHOLD
                ? Instant.ofEpochSecond(value)
                : Instant.ofEpochMilli(value);
    }
}
