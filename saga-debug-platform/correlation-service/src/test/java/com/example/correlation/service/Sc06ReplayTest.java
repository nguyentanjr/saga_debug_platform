package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.*;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Sc06ReplayTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static Path repoRoot() {
        Path here = Path.of("").toAbsolutePath();
        return Files.exists(here.resolve("oracle/sc06-observations.json")) ? here : here.getParent();
    }

    @Test
    void replayIsRepeatableAndPreservesStoredObservations() throws Exception {
        Path root = repoRoot();
        Path input = root.resolve("oracle/sc06-observations.json");
        JsonNode fixture = mapper.readTree(input.toFile());
        ArrayNode rows = mapper.createArrayNode();
        assertThat(fixture.path("raw_runs").size()).isEqualTo(3);
        for (JsonNode run : fixture.path("raw_runs")) {
            ObjectNode first = replay(run);

            assertThat(replay(run)).isEqualTo(first);
            rows.add(first);
        }
        String output = System.getProperty("sc06.output");
        if (output != null) {
            ObjectNode report = mapper.createObjectNode();
            ObjectNode metadata = report.putObject("metadata");
            metadata.put("scope", "SC06 correlation+inline-detectors+RCA; in-memory repositories; no sweeps");
            metadata.put("input_sha256", sha256(Files.readAllBytes(input)));
            metadata.put("source_artifact_sha256", fixture.path("source_sha256").asText());
            metadata.put("delivery_order", "receivedAt ascending; ties rejected");
            metadata.put("decision_point", "immediately after final stored observation; no wall-clock sweeps");
            metadata.put("java_version", System.getProperty("java.version"));
            metadata.set("detection_properties", mapper.valueToTree(new DetectionProperties()));
            ObjectNode hashes = metadata.putObject("resource_sha256");
            Path resources = root.resolve("correlation-service/src/main/resources");
            try (var paths = Files.walk(resources)) {
                for (Path path : paths.filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                        .sorted().toList()) {
                    hashes.put(resources.relativize(path).toString().replace('\\', '/'),
                            sha256(Files.readAllBytes(path)));
                }
            }
            report.set("raw_runs", rows);
            Path destination = Path.of(output).toAbsolutePath();
            if (Files.exists(destination)) throw new IllegalArgumentException("Refusing overwrite: " + destination);
            Files.createDirectories(destination.getParent());
            mapper.writerWithDefaultPrettyPrinter().writeValue(destination.toFile(), report);
        }
    }

    private ObjectNode replay(JsonNode run) throws Exception {
        Map<String, SagaInstance> instances = new LinkedHashMap<>();
        List<SagaEvent> events = new ArrayList<>();
        List<Anomaly> anomalies = new ArrayList<>();
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        AnomalyRepository anomalyRepo = mock(AnomalyRepository.class);
        RcaResultRepository rcaRepo = mock(RcaResultRepository.class);
        when(sagas.findById(anyString())).thenAnswer(c -> Optional.ofNullable(instances.get(c.getArgument(0))));
        when(sagas.findByIdForUpdate(anyString())).thenAnswer(c -> Optional.ofNullable(instances.get(c.getArgument(0))));
        when(sagas.save(any())).thenAnswer(c -> {
            SagaInstance saga = c.getArgument(0); instances.put(saga.getSagaId(), saga); return saga;
        });
        when(eventRepo.findBySourceNameAndEventId(anyString(), anyString())).thenAnswer(c ->
                events.stream().filter(e -> e.getSourceName().equals(c.getArgument(0))
                        && e.getEventId().equals(c.getArgument(1))).findFirst());
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenAnswer(c ->
                events.stream().filter(e -> e.getSagaId().equals(c.getArgument(0)))
                        .sorted(Comparator.comparing(SagaEvent::getTimestamp)).toList());
        when(eventRepo.save(any())).thenAnswer(c -> {
            SagaEvent event = c.getArgument(0); events.add(event); return event;
        });
        when(anomalyRepo.save(any())).thenAnswer(c -> {
            Anomaly anomaly = c.getArgument(0); anomaly.setId((long) anomalies.size() + 1);
            anomalies.add(anomaly); return anomaly;
        });
        when(anomalyRepo.existsBySagaIdAndAnomalyType(anyString(), any())).thenAnswer(c ->
                anomalies.stream().anyMatch(a -> a.getSagaId().equals(c.getArgument(0))
                        && a.getAnomalyType() == c.getArgument(1) && a.getRetractedAt() == null));
        when(anomalyRepo.findStandingBySagaId(anyString())).thenAnswer(c ->
                anomalies.stream().filter(a -> a.getSagaId().equals(c.getArgument(0))
                        && a.getRetractedAt() == null).toList());
        when(rcaRepo.save(any())).thenAnswer(c -> c.getArgument(0));

        WorkflowRegistry workflows = new WorkflowRegistry(); workflows.load();
        RuleRegistry rules = new RuleRegistry(); rules.load();
        DetectionProperties properties = new DetectionProperties();
        ExecutionProjectionService projection = new ExecutionProjectionService();
        AnomalyDetectionService detector = new AnomalyDetectionService(
                sagas, eventRepo, anomalyRepo, workflows, mapper, properties, projection);
        CorrelationService correlation = new CorrelationService(
                sagas, eventRepo, detector, new DeduplicationCache(), workflows, mapper);

        FailureSignalRegistry failureSignals =
                new FailureSignalRegistry(repoRoot().resolve("declarations").toString());
        failureSignals.load();
        RcaService rca = new RcaService(sagas, eventRepo, anomalyRepo, rcaRepo, workflows,
                mapper, new RcaSelector(), rules, properties, mock(AlertSink.class), projection,
                failureSignals, null);

        List<JsonNode> observations = new ArrayList<>();
        run.path("raw_events").forEach(observations::add);
        assertThat(observations).hasSize(5);
        observations.sort(Comparator.comparing(e -> Instant.parse(e.path("receivedAt").asText())));
        assertThat(observations.stream().map(e -> e.path("receivedAt").asText()).distinct().count())
                .as("capture must establish delivery order without invented tie-breaks").isEqualTo(5);
        for (JsonNode event : observations) {
            JsonNode payload = mapper.readTree(event.path("payload").asText());
            String source = event.path("sourceName").asText();
            ExecutionKey key = ExecutionKey.of(source, payload.path("sagaId").asText());
            assertThat(key.canonical()).isEqualTo(run.path("execution_id").asText());
            correlation.processEvent(new IngestedEvent(
                    new IngestedEvent.Identity(source, key, event.path("eventId").asText(),
                            nullable(payload, "orderId"), CorrelationBasis.valueOf(event.path("correlationBasis").asText())),
                    null, event.path("eventType").asText(), nullable(event, "stepName"),
                    nullable(event, "serviceName"), Instant.parse(event.path("timestamp").asText()),
                    event.path("payload").asText(), Instant.parse(event.path("receivedAt").asText()),
                    new IngestedEvent.Causality(nullable(event, "causationId"), nullable(event, "spanId"),
                            nullable(event, "parentSpanId"), nullable(event, "traceId")), nullable(event, "transportRef")));
        }
        assertThat(events).hasSize(observations.size());
        for (int i = 0; i < events.size(); i++) {
            JsonNode stored = mapper.valueToTree(events.get(i));
            for (String field : List.of("payload", "eventId", "eventType", "sourceName", "transportRef",
                    "causationId", "spanId", "parentSpanId", "traceId", "correlationBasis")) {
                assertThat(stored.path(field)).as("preserved %s", field).isEqualTo(observations.get(i).path(field));
            }
            assertThat(events.get(i).getTimestamp()).isEqualTo(Instant.parse(observations.get(i).path("timestamp").asText()));
            assertThat(events.get(i).getReceivedAt()).isEqualTo(Instant.parse(observations.get(i).path("receivedAt").asText()));
        }
        String id = run.path("execution_id").asText();
        RcaResult result = rca.analyze(id);
        assertThat(result).isNotNull();
        assertThat(instances.get(id).getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(anomalies).extracting(Anomaly::getAnomalyType).containsExactly(AnomalyType.RETRY_BURST);
        ObjectNode row = mapper.createObjectNode();
        row.put("scenario_id", run.path("scenario_id").asText());
        row.put("run_index", run.path("run_index").asInt());
        row.set("raw_events", run.path("raw_events").deepCopy());
        row.put("execution_id", id);
        row.put("saga_status", instances.get(id).getStatus().name());
        row.put("rca_status", result.getRcaStatus().name());
        row.put("rca_root_cause", result.getRootCause());
        row.set("rca_primary_roots", mapper.valueToTree(result.getPrimaryRootCauses()));
        row.set("rca_candidates", mapper.valueToTree(result.getCandidates()));
        row.set("anomalies_actual", mapper.valueToTree(anomalies.stream().map(a -> a.getAnomalyType().name()).toList()));

        row.put("rca_accurate", "CIRCUIT_BREAKER_OPEN".equals(result.getRootCause()));
        return row;
    }

    private String nullable(JsonNode node, String key) {
        return node.path(key).isMissingNode() || node.path(key).isNull() ? null : node.path(key).asText();
    }

    private String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
