package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.ingestion.ExecutionAliasResolver;
import com.example.correlation.ingestion.RawObservation;
import com.example.correlation.ingestion.SourceMapping;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BackfillReplayTest {
    @Test
    void theTransferRunsPastRetentionReadAsReported() throws Exception {
        ObjectMapper om = new ObjectMapper();
        Path decl = CorrectnessAblationTest.repoRoot().resolve("declarations");
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", decl.toString());
        workflows.load();
        SourceMappingRegistry mappings = new SourceMappingRegistry();
        ReflectionTestUtils.setField(mappings, "declarationsDirectory", decl.toString());
        mappings.load();
        ReplayPipeline p = new ReplayPipeline(workflows);
        when(p.sagas.findByStatusInAndLastUpdatedBefore(any(), any(), any())).thenAnswer(c -> {
            Collection<SagaStatus> st = c.getArgument(0);
            Instant before = c.getArgument(1);
            return p.instances.values().stream()
                    .filter(s -> st.contains(s.getStatus()) && s.getLastUpdated() != null && s.getLastUpdated().isBefore(before))
                    .toList();
        });
        when(p.eventRepo.countBySagaId(anyString())).thenAnswer(c ->
                p.events.stream().filter(e -> e.getSagaId().equals(c.getArgument(0))).count());

        Map<ExecutionAlias.Key, ExecutionAlias> table = new HashMap<>();
        ExecutionAliasRepository aliases = mock(ExecutionAliasRepository.class);
        when(aliases.findById(any())).thenAnswer(c -> Optional.ofNullable(table.get((ExecutionAlias.Key) c.getArgument(0))));
        when(aliases.save(any())).thenAnswer(c -> {
            ExecutionAlias a = c.getArgument(0);
            table.put(new ExecutionAlias.Key(a.getSourceName(), a.getAliasKey()), a); return a;
        });
        when(aliases.findByExecutionId(anyString())).thenAnswer(c -> table.values().stream()
                .filter(a -> a.getExecutionId().equals(c.getArgument(0))).toList());
        ExecutionAliasResolver resolver = new ExecutionAliasResolver(aliases, p.eventRepo, p.sagas,
                p.anomalyRepo, mock(RcaResultRepository.class));

        for (JsonNode n : om.readTree(getClass().getResourceAsStream("/ewallet-staging/events-24-runs.json"))) {
            String ref = n.get("ref").asText();
            String part = ref.substring(0, ref.lastIndexOf('-'));
            String topic = part.substring(0, part.lastIndexOf('-'));
            Instant ts = Instant.parse(n.get("ts").asText());
            JsonNode raw0 = n.get("payload");
            String body = raw0.isTextual() ? raw0.asText() : raw0.toString();
            SourceMapping m = mappings.forTopic(topic);
            RawObservation raw = new RawObservation(topic, body, ts, ref, null, Map.of());
            JsonNode payload = om.readTree(body);
            ExecutionKey declared = ExecutionKey.of(m.getSystem(), m.executionId(payload, raw).orElseThrow());
            ExecutionKey alias = m.aliasKey(payload, raw).map(k -> ExecutionKey.of(m.aliasNamespace(), k)).orElse(null);
            ExecutionKey key = resolver.canonical(declared, alias, ref, ts);
            p.deliver(List.of(new IngestedEvent(
                    new IngestedEvent.Identity(key.source(), key, ref, m.businessKey(payload, raw).orElse(null),
                            key.equals(declared) ? CorrelationBasis.DECLARED_KEY : CorrelationBasis.ALIAS_LINK),
                    m.getWorkflow(), m.eventType(payload, raw).orElse(topic), "UNKNOWN", "svc", ts, body,
                    ts, IngestedEvent.Causality.NONE, ref)));
        }
        p.detector.detectStuckSagas();
        p.detector.detectMissingTerminalEvents();

        List<Map<String, Object>> out = new ArrayList<>();
        p.instances.values().stream().sorted(Comparator.comparing(SagaInstance::getSagaId)).forEach(s -> {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("sagaId", s.getSagaId());
            r.put("workflowType", s.getWorkflowType());
            r.put("status", s.getStatus().name());
            r.put("startTime", String.valueOf(s.getStartTime()));
            r.put("events", p.events.stream().filter(e -> e.getSagaId().equals(s.getSagaId())).map(SagaEvent::getTransportRef).toList());
            r.put("anomalies", p.anomalies.stream().filter(a -> a.getSagaId().equals(s.getSagaId()) && a.getRetractedAt() == null)
                    .map(a -> a.getAnomalyType().name()).sorted().toList());
            out.add(r);
        });
        Map<String, Long> byOutcome = new TreeMap<>();
        out.forEach(r -> byOutcome.merge(r.get("workflowType") + " " + r.get("status") + " " + r.get("anomalies"), 1L, Long::sum));

        assertThat(byOutcome).containsExactly(
                Map.entry("EWALLET_W2N_TRANSFER COMPENSATED []", 13L),
                Map.entry("EWALLET_W2N_TRANSFER COMPLETED []", 4L),
                Map.entry("EWALLET_W2O_TRANSFER COMPLETED []", 2L),
                Map.entry("UNRESOLVED IN_PROGRESS [MISSING_TERMINAL_EVENT, ORPHAN_EVENT, SAGA_STUCK]", 5L));
    }
}
