package com.example.correlation.controller;

import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;

@RestController
@RequestMapping("/api/lifecycle-events")
public class LifecycleEventController {
    private final ServiceLifecycleEventRepository repository;
    private final IngestAuthorization authorization;

    public LifecycleEventController(ServiceLifecycleEventRepository repository,
            IngestAuthorization authorization) {
        this.repository = repository;
        this.authorization = authorization;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> record(@RequestBody JsonNode body,
            @RequestHeader(value = "Authorization", required = false) String authorizationHeader) {
        ResponseEntity<Map<String, Object>> refused = authorization.refusal(authorizationHeader);
        if (refused != null) return refused;

        String service = text(body, "service");
        if (service == null) return badRequest("'service' is required");

        ServiceLifecycleEvent.State state;
        try {
            state = ServiceLifecycleEvent.State.valueOf(upper(text(body, "state")));
        } catch (IllegalArgumentException | NullPointerException e) {
            return badRequest("'state' must be DOWN or UP");
        }

        ServiceLifecycleEvent.Basis basis;
        try {
            basis = ServiceLifecycleEvent.Basis.valueOf(upper(text(body, "basis")));
        } catch (IllegalArgumentException | NullPointerException e) {
            return badRequest("'basis' must be 'process' (the runtime saw the process exit/start) "
                    + "or 'probe' (something outside could or could not reach it)");
        }

        Instant at;
        try {
            String raw = text(body, "at");
            if (raw == null) return badRequest("'at' is required (ISO-8601 instant)");
            at = Instant.parse(raw);
        } catch (DateTimeParseException e) {
            return badRequest("'at' must be an ISO-8601 instant, e.g. 2026-10-04T10:00:05Z");
        }

        String source = text(body, "source") != null ? text(body, "source") : "api";
        String id = text(body, "id") != null ? text(body, "id") : service + ":" + state + ":" + at;

        if (repository.existsBySourceAndExternalId(source, id)) {
            return ResponseEntity.ok(Map.of("status", "duplicate", "id", id));
        }
        try {
            repository.save(new ServiceLifecycleEvent(service, state, at, source,
                    text(body, "detail"), id, basis).inScope(text(body, "scope")));
        } catch (DataIntegrityViolationException concurrentDuplicate) {
            return ResponseEntity.ok(Map.of("status", "duplicate", "id", id));
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("status", "recorded", "id", id));
    }

    private static String text(JsonNode body, String field) {
        if (body == null) return null;
        JsonNode node = body.get(field);
        if (node == null || node.isNull()) return null;
        String value = node.asText().trim();
        return value.isEmpty() ? null : value;
    }

    private static String upper(String value) {
        return value == null ? null : value.toUpperCase(Locale.ROOT);
    }

    private static ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
