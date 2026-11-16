package com.example.correlation.ingestion;

import java.time.Instant;
import java.util.Map;

public record RawObservation(
        String sourceRef,
        String payload,
        Instant transportTime,
        String fallbackEventId,
        String key,
        Map<String, String> headers,
        String origin) {
    public RawObservation {
        headers = headers != null ? Map.copyOf(headers) : Map.of();
    }

    public RawObservation(String sourceRef, String payload, Instant transportTime,
                          String fallbackEventId, String key, Map<String, String> headers) {
        this(sourceRef, payload, transportTime, fallbackEventId, key, headers, null);
    }

    public static RawObservation of(String sourceRef, String payload, Instant transportTime) {
        return new RawObservation(sourceRef, payload, transportTime, null, null, Map.of(), null);
    }

    public String header(String name) {
        if (name == null) {
            return null;
        }
        String exact = headers.get(name);
        if (exact != null) {
            return exact;
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey().equalsIgnoreCase(name)) {
                return header.getValue();
            }
        }
        return null;
    }
}
