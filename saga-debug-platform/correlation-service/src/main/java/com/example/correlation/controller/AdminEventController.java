package com.example.correlation.controller;

import com.example.correlation.domain.EventTimeSource;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.RcaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/admin")
public class AdminEventController {
    private static final Logger log = LoggerFactory.getLogger(AdminEventController.class);

    private final CorrelationService correlationService;
    private final RcaService rcaService;
    private final ExecutionLookup executionLookup;
    private final IngestAuthorization authorization;

    public AdminEventController(CorrelationService correlationService, RcaService rcaService,
            ExecutionLookup executionLookup, IngestAuthorization authorization) {
        this.correlationService = correlationService;
        this.rcaService = rcaService;
        this.executionLookup = executionLookup;
        this.authorization = authorization;
    }

    @PostMapping("/inject-event")
    public ResponseEntity<Map<String, String>> injectEvent(@RequestBody Map<String, String> body,
            @RequestHeader(value = "Authorization", required = false) String authorizationHeader) {
        ResponseEntity<Map<String, String>> refused = authorization.refusal(authorizationHeader);
        if (refused != null) return refused;

        String sagaId = body.getOrDefault("sagaId", UUID.randomUUID().toString());
        String eventType = body.get("eventType");
        String stepName = body.getOrDefault("stepName", "UNKNOWN");
        String serviceName = body.getOrDefault("serviceName", "test-inject");
        String payload = body.getOrDefault("payload", "{}");
        String eventId = body.getOrDefault("eventId",
                "inject-" + UUID.randomUUID().toString().substring(0, 8));
        String timestampStr = body.get("timestamp");

        if (eventType == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "eventType is required"));
        }

        Instant timestamp = timestampStr != null
                ? Instant.parse(timestampStr)
                : Instant.now();

        com.example.correlation.domain.ExecutionKey key = com.example.correlation.domain.ExecutionKey
                .of(com.example.correlation.domain.ExecutionKey.DEFAULT_SOURCE, sagaId);

        correlationService.processEvent(new com.example.correlation.domain.IngestedEvent(
                new com.example.correlation.domain.IngestedEvent.Identity(
                        key.source(), key, eventId, body.get("orderId"),
                        com.example.correlation.domain.CorrelationBasis.DECLARED_KEY),
                null, eventType, stepName, serviceName, timestamp, payload, Instant.now(),
                new com.example.correlation.domain.IngestedEvent.Causality(
                        body.get("causationId"), body.get("spanId"),
                        body.get("parentSpanId"), body.get("traceId")),
                null, timestampStr != null ? EventTimeSource.DECLARED : EventTimeSource.TRANSPORT));

        log.info("Injected event: sagaId={}, eventType={}", sagaId, eventType);

        return ResponseEntity.ok(Map.of(
                "status", "injected",
                "sagaId", sagaId,
                "executionId", key.canonical(),
                "eventId", eventId,
                "eventType", eventType
        ));
    }

    @PostMapping("/analyze/{sagaId}")
    public ResponseEntity<?> triggerRca(@PathVariable("sagaId") String sagaId) {
        String executionId = executionLookup.resolve(sagaId).orElse(null);
        if (executionId == null) {
            return ResponseEntity.notFound().build();
        }
        var result = rcaService.analyze(executionId);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }
}
