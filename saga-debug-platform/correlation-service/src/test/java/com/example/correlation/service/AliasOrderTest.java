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

import static org.assertj.core.api.Assertions.assertThat;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Staging replay: one group of runs through the real alias
 * resolver, in every delivery order that keeps each partition's offset order.
 *
 * <p>-DaliasEvents=file.json: [{ref, ts, payload}], ref = topic-partition-offset.
 * -DnoLink=true drops the alias of the record whose ref ends in -DnoLinkRef (control arm).
 * Mirrors EventIngestionService lines 168-190; no dedup, no transaction. Received time is
 * the harness's wall clock, so SAGA_STUCK is not raised here; MISSING_TERMINAL_EVENT is
 * not either, ReplayPipeline does not answer findByStatusInAndLastUpdatedBefore.
 */
class AliasOrderTest {

    record Ev(String ref, String topic, String partition, Instant ts, String payload) {}

    /** Staging 2026-11-02: hold A6305591851880725 and the release M6305592191590003 that names it. */
    private static final String EVENTS = "/ewallet-staging/events-alias-T67.json";
    private static final String LINK = "staging-ewallet-transfer-management-v2-2-1183311";

    @Test
    void theReleaseJoinsItsHoldInEveryDeliveryOrder() throws Exception {
        Map<String, List<Integer>> outcomes = outcomes("");
        assertThat(outcomes.values().stream().mapToInt(List::size).sum()).isEqualTo(420);
        assertThat(outcomes.keySet()).containsExactly("A6305591851880725[EWALLET_W2N_TRANSFER COMPLETED 7ev []]");
    }

    /** Without the event that carries both ids the two stay apart, as the old build read them. */
    @Test
    void withoutTheLinkingEventTheyStayApartInEveryOrder() throws Exception {
        assertThat(outcomes(LINK).keySet()).singleElement().asString()
                .startsWith("A6305591851880725[EWALLET_W2N_TRANSFER IN_PROGRESS 4ev")
                .contains("M6305592191590003[UNRESOLVED");
    }

    private Map<String, List<Integer>> outcomes(String unlinked) throws Exception {
        ObjectMapper om = new ObjectMapper();
        List<Ev> evs = new ArrayList<>();
        for (JsonNode n : om.readTree(getClass().getResourceAsStream(EVENTS))) {
            String ref = n.get("ref").asText();
            String part = ref.substring(0, ref.lastIndexOf('-'));
            String topic = part.substring(0, part.lastIndexOf('-'));
            JsonNode body = n.get("payload");
            evs.add(new Ev(ref, topic, part, Instant.parse(n.get("ts").asText()),
                    body.isTextual() ? body.asText() : body.toString()));
        }
        Map<String, List<Ev>> byPart = new LinkedHashMap<>();
        evs.forEach(e -> byPart.computeIfAbsent(e.partition(), k -> new ArrayList<>()).add(e));
        byPart.values().forEach(l -> l.sort(Comparator.comparingLong(e -> Long.parseLong(e.ref().substring(e.ref().lastIndexOf('-') + 1)))));
        List<List<Ev>> orders = new ArrayList<>();
        interleave(new ArrayList<>(byPart.values()), new int[byPart.size()], new ArrayList<>(), orders);

        Map<String, List<Integer>> outcomes = new TreeMap<>();
        for (int i = 0; i < orders.size(); i++) {
            outcomes.computeIfAbsent(run(orders.get(i), unlinked), k -> new ArrayList<>()).add(i);
        }
        return outcomes;
    }

    static void interleave(List<List<Ev>> parts, int[] idx, List<Ev> cur, List<List<Ev>> out) {
        boolean done = true;
        for (int p = 0; p < parts.size(); p++) {
            if (idx[p] < parts.get(p).size()) {
                done = false;
                cur.add(parts.get(p).get(idx[p])); idx[p]++;
                interleave(parts, idx, cur, out);
                idx[p]--; cur.remove(cur.size() - 1);
            }
        }
        if (done) out.add(new ArrayList<>(cur));
    }

    String run(List<Ev> order, String unlinked) throws Exception {
        Path decl = CorrectnessAblationTest.repoRoot().resolve("declarations");
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", decl.toString());
        workflows.load();
        SourceMappingRegistry mappings = new SourceMappingRegistry();
        ReflectionTestUtils.setField(mappings, "declarationsDirectory", decl.toString());
        mappings.load();
        ReplayPipeline p = new ReplayPipeline(workflows);

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

        ObjectMapper om = new ObjectMapper();
        for (Ev e : order) {
            SourceMapping m = mappings.forTopic(e.topic());
            RawObservation raw = new RawObservation(e.topic(), e.payload(), e.ts(), e.ref(), null, Map.of());
            JsonNode payload = om.readTree(e.payload());
            ExecutionKey declared = ExecutionKey.of(m.getSystem(), m.executionId(payload, raw).orElseThrow());
            ExecutionKey alias = !unlinked.isEmpty() && e.ref().equals(unlinked)
                    ? null
                    : m.aliasKey(payload, raw).map(k -> ExecutionKey.of(m.aliasNamespace(), k)).orElse(null);
            ExecutionKey key = resolver.canonical(declared, alias, e.ref(), e.ts());
            p.deliver(List.of(new IngestedEvent(
                    new IngestedEvent.Identity(key.source(), key, e.ref(), null,
                            key.equals(declared) ? CorrelationBasis.DECLARED_KEY : CorrelationBasis.ALIAS_LINK),
                    m.getWorkflow(), m.eventType(payload, raw).orElse(e.topic()), "UNKNOWN", "svc", e.ts(), "{}",
                    Instant.now(), IngestedEvent.Causality.NONE, e.ref())));
        }
        p.detector.detectStuckSagas();
        StringBuilder sb = new StringBuilder();
        p.instances.values().stream().sorted(Comparator.comparing(SagaInstance::getSagaId)).forEach(s -> {
            long n = p.events.stream().filter(e -> e.getSagaId().equals(s.getSagaId())).count();
            sb.append(s.getSagaId().replaceAll(".*::", "")).append('[').append(s.getWorkflowType()).append(' ')
                    .append(s.getStatus()).append(' ').append(n).append("ev ")
                    .append(p.anomalies.stream().filter(a -> a.getSagaId().equals(s.getSagaId()) && a.getRetractedAt() == null)
                            .map(a -> a.getAnomalyType().name()).sorted().toList()).append("] ");
        });
        return sb.toString().trim();
    }
}
