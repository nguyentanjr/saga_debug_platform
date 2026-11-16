package com.example.correlation.service.rca;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class FailureSignalDeclaration {
    private static final ObjectMapper JSON = new ObjectMapper();

    private String source;

    private List<String> reasonFields = List.of("reason");

    private Map<FailureSignal, List<String>> signals = new LinkedHashMap<>();

    public Optional<String> readReason(String payload) {
        if (payload == null || payload.isBlank()) return Optional.empty();
        JsonNode root;
        try {
            root = JSON.readTree(payload);
        } catch (Exception e) {
            return Optional.empty();
        }
        for (String field : reasonFields) {
            JsonNode node = root.get(field);
            if (node != null && node.isTextual() && !node.asText().isBlank()) {
                return Optional.of(node.asText());
            }
        }
        return Optional.empty();
    }

    public Optional<String> matchedPattern(FailureSignal signal, String reason) {
        if (reason == null) return Optional.empty();
        String haystack = reason.toLowerCase(Locale.ROOT);
        return signals.getOrDefault(signal, List.of()).stream()
                .filter(pattern -> pattern != null && !pattern.isBlank())
                .filter(pattern -> haystack.contains(pattern.toLowerCase(Locale.ROOT)))
                .findFirst();
    }

    public boolean declares(FailureSignal signal) {
        List<String> patterns = signals.get(signal);
        return patterns != null && !patterns.isEmpty();
    }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public List<String> getReasonFields() { return reasonFields; }
    public void setReasonFields(List<String> reasonFields) {
        if (reasonFields != null && !reasonFields.isEmpty()) this.reasonFields = reasonFields;
    }

    public Map<FailureSignal, List<String>> getSignals() { return signals; }
    public void setSignals(Map<FailureSignal, List<String>> signals) {
        this.signals = signals == null ? new LinkedHashMap<>() : signals;
    }
}
