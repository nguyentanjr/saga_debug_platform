package com.example.correlation.controller;

import com.example.correlation.ingestion.EventIngestionService;
import com.example.correlation.ingestion.IngestionResult;
import com.example.correlation.ingestion.RawObservation;
import com.example.correlation.ingestion.SourceMappingInferrer;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@RestController
@RequestMapping("/api/ingest")
public class IngestController {
    private static final Logger log = LoggerFactory.getLogger(IngestController.class);

    private static final int MAX_BATCH = 1000;

    private final EventIngestionService ingestionService;
    private final SourceMappingInferrer inferrer;
    private final IngestAuthorization authorization;

    public IngestController(EventIngestionService ingestionService, SourceMappingInferrer inferrer,
                            IngestAuthorization authorization) {
        this.ingestionService = ingestionService;
        this.inferrer = inferrer;
        this.authorization = authorization;
    }

    @PostMapping("/{source}/infer")
    public ResponseEntity<Map<String, Object>> infer(@PathVariable("source") String source,
            @RequestBody JsonNode body,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        ResponseEntity<Map<String, Object>> refused = this.authorization.refusal(authorization);
        if (refused != null) return refused;

        List<JsonNode> sample = body.isArray() ? toList(body) : List.of(body);
        if (sample.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "no events to learn from"));
        }

        SourceMappingInferrer.InferredMapping inferred = inferrer.infer(sample);

        Map<String, Object> fields = new LinkedHashMap<>();
        inferred.fields().forEach((field, proposal) -> fields.put(field, Map.of(
                "path", proposal.path(),
                "basis", proposal.basis(),
                "confidence", proposal.confidence())));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("source", source);
        response.put("sampleSize", inferred.sampleSize());
        response.put("fields", fields);
        response.put("observedEventTypes", inferred.observedEventTypes());
        response.put("unresolved", inferred.unresolved());
        response.put("warnings", inferred.warnings());

        response.put("proposedYaml",
                inferred.toYaml(source, source.replace(".", "\\.")));

        return ResponseEntity.ok(response);
    }

    public static final String KEY_HEADER = "X-Observation-Key";

    @PostMapping("/{source}")
    public ResponseEntity<Map<String, Object>> ingest(@PathVariable("source") String source,
            @RequestBody JsonNode body,
            @RequestHeader Map<String, String> headers) {
        ResponseEntity<Map<String, Object>> refused = authorization.refusal(headerIgnoringCase(headers, "Authorization"));
        if (refused != null) return refused;

        List<JsonNode> events = body.isArray()
                ? toList(body)
                : List.of(body);

        if (events.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "no events in request"));
        }
        if (events.size() > MAX_BATCH) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "batch too large",
                    "max", MAX_BATCH,
                    "received", events.size()));
        }

        Instant receivedAt = Instant.now();
        List<Map<String, Object>> results = new ArrayList<>(events.size());
        int accepted = 0;
        int duplicates = 0;
        int rejected = 0;

        Map<String, String> transportHeaders = normalise(headers);
        String key = transportHeaders.get(KEY_HEADER.toLowerCase(Locale.ROOT));

        for (JsonNode event : events) {
            IngestionResult result = ingestionService.ingest(new RawObservation(
                    source, event.toString(), receivedAt, null, key, transportHeaders));

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("outcome", result.outcome().name());
            if (result.eventId() != null) entry.put("eventId", result.eventId());
            if (result.reason() != null) entry.put("reason", result.reason());
            results.add(entry);

            if (result.isAccepted()) accepted++;
            else if (result.outcome() == IngestionResult.Outcome.DUPLICATE) duplicates++;
            else rejected++;
        }

        log.info("Ingested {} observation(s) from source '{}': {} accepted, {} duplicate, {} rejected",
                events.size(), source, accepted, duplicates, rejected);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("source", source);
        response.put("received", events.size());
        response.put("accepted", accepted);
        response.put("duplicates", duplicates);
        response.put("rejected", rejected);
        response.put("results", results);

        return ResponseEntity
                .status(accepted > 0 || duplicates > 0 ? HttpStatus.ACCEPTED : HttpStatus.BAD_REQUEST)
                .body(response);
    }

    static Map<String, String> normalise(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        Map<String, String> normalised = new LinkedHashMap<>();
        headers.forEach((name, value) -> {
            if (name != null && value != null && !name.equalsIgnoreCase("Authorization")) {
                normalised.put(name.toLowerCase(Locale.ROOT), value);
            }
        });
        return normalised;
    }

    private static String headerIgnoringCase(Map<String, String> headers, String name) {
        if (headers == null) return null;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (name.equalsIgnoreCase(entry.getKey())) return entry.getValue();
        }
        return null;
    }

    private static List<JsonNode> toList(JsonNode array) {
        List<JsonNode> events = new ArrayList<>(array.size());
        array.forEach(events::add);
        return events;
    }
}
