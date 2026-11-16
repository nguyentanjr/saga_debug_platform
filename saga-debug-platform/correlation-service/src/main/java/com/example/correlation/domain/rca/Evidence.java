package com.example.correlation.domain.rca;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Evidence(
        String fact,
        String observedValue,
        String expectedValue,
        Long anomalyId,
        List<String> eventIds) {
    public Evidence {
        eventIds = eventIds == null ? List.of() : List.copyOf(eventIds);
    }

    public static Evidence of(String fact, String observedValue, String expectedValue,
            Long anomalyId, List<String> eventIds) {
        return new Evidence(fact, observedValue, expectedValue, anomalyId, eventIds);
    }

    public static Evidence of(String fact, Long anomalyId) {
        return new Evidence(fact, null, null, anomalyId, List.of());
    }

    public static Evidence ofEvent(String fact, String observedValue, String expectedValue,
            String eventId) {
        return new Evidence(fact, observedValue, expectedValue, null, List.of(eventId));
    }
}
