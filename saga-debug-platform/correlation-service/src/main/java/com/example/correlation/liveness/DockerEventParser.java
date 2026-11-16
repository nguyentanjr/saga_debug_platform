package com.example.correlation.liveness;

import com.example.correlation.domain.ServiceLifecycleEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Optional;

public final class DockerEventParser {
    public static final String SOURCE = "docker";

    static final String PROJECT_LABEL = "com.docker.compose.project";

    private final ObjectMapper objectMapper;
    private final String serviceLabel;

    public DockerEventParser(ObjectMapper objectMapper, String serviceLabel) {
        this.objectMapper = objectMapper;
        this.serviceLabel = serviceLabel;
    }

    public Optional<ServiceLifecycleEvent> parse(String line) {
        if (line == null || !line.startsWith("{")) return Optional.empty();
        JsonNode node;
        try {
            node = objectMapper.readTree(line);
        } catch (Exception e) {
            return Optional.empty();
        }
        if (!"container".equals(node.path("Type").asText())) return Optional.empty();

        String action = node.path("Action").asText();
        ServiceLifecycleEvent.State state = switch (action) {
            case "die" -> ServiceLifecycleEvent.State.DOWN;
            case "start" -> ServiceLifecycleEvent.State.UP;
            default -> null;
        };
        if (state == null) return Optional.empty();

        JsonNode attributes = node.path("Actor").path("Attributes");
        String service = attributes.path(serviceLabel).asText("");
        if (service.isBlank()) service = attributes.path("name").asText("");
        if (service.isBlank()) return Optional.empty();

        long timeNano = node.path("timeNano").asLong(0);
        if (timeNano <= 0) return Optional.empty();
        Instant occurredAt = Instant.ofEpochSecond(0, timeNano);

        String detail = action;
        String exitCode = attributes.path("exitCode").asText("");
        if (!exitCode.isBlank()) detail += " exitCode=" + exitCode;

        String containerId = node.path("Actor").path("ID").asText(node.path("id").asText(""));
        String externalId = containerId + ":" + action + ":" + timeNano;
        String scope = attributes.path(PROJECT_LABEL).asText("");
        return Optional.of(new ServiceLifecycleEvent(service, state, occurredAt, SOURCE, detail, externalId)
                .inScope(scope));
    }
}
