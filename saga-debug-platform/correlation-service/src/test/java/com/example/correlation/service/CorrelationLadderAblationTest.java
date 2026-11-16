package com.example.correlation.service;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionAlias;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.ingestion.EventIngestionService;
import com.example.correlation.ingestion.ExecutionAliasResolver;
import com.example.correlation.ingestion.InlineTransactionManager;
import com.example.correlation.ingestion.IngestionResult;
import com.example.correlation.ingestion.RawObservation;
import com.example.correlation.ingestion.SourceCompletenessCheck;
import com.example.correlation.ingestion.SourceMapping;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CorrelationLadderAblationTest {
    private static final Instant T0 = Instant.parse("2026-09-01T10:00:00Z");

    private static final String TRACE_ONLY = """
            name: trace-shop
            topicPattern: 'trace-shop\\.events'
            fields:
              eventId: id
              eventType: type
              eventTime: at
              traceId: traceparentTraceId
            """;

    @Test
    void traceContextIsTheDifferenceBetweenObservableAndNot() {
        Arm control = new Arm(TRACE_ONLY);
        List<IngestionResult> withTrace = control.ingestAll("trace-shop.events", healthyRun("t", "4bf92f3577b34da6a3ce929d0e0e4736"));

        Arm ablated = new Arm(TRACE_ONLY);
        List<IngestionResult> withoutTrace = ablated.ingestAll("trace-shop.events", healthyRun("t", null));

        report("A trace", "control", control, withTrace);
        report("A trace", "ablated (no trace)", ablated, withoutTrace);

        assertThat(control.pipeline.instances).hasSize(1);
        assertThat(control.pipeline.events).extracting(SagaEvent::getCorrelationBasis)
                .containsOnly(CorrelationBasis.PROPAGATED_CONTEXT);
        String id = control.pipeline.instances.keySet().iterator().next();
        ReplayPipeline.Outcome o = control.pipeline.outcome(id);
        assertThat(o.sagaStatus).isEqualTo("COMPLETED");
        assertThat(o.rcaStatus).isEqualTo("NO_DIAGNOSIS_NEEDED");

        assertThat(withoutTrace).allSatisfy(r ->
                assertThat(r.outcome()).isEqualTo(IngestionResult.Outcome.NOT_CORRELATABLE));
        assertThat(ablated.pipeline.instances).isEmpty();
    }

    private static final String ORDERS = """
            name: split-shop-orders
            system: split-shop
            topicPattern: 'orders\\.events'
            fields:
              eventId: id
              executionId: orderNo
              eventType: type
              eventTime: at
            """;

    private static final String PAYMENTS_WITH_ALIAS = """
            name: split-shop-payments
            system: split-shop
            topicPattern: 'payments\\.events'
            fields:
              eventId: id
              executionId: paymentRef
              aliasKey: orderNo
              eventType: type
              eventTime: at
            """;

    private static final String PAYMENTS_WITHOUT_ALIAS = """
            name: split-shop-payments
            system: split-shop
            topicPattern: 'payments\\.events'
            fields:
              eventId: id
              executionId: paymentRef
              eventType: type
              eventTime: at
            """;

    @Test
    void aliasLinkJoinsServicesThatKnowDifferentIds() {
        Arm control = new Arm(ORDERS, PAYMENTS_WITH_ALIAS);
        List<IngestionResult> c = splitRun(control);

        Arm ablated = new Arm(ORDERS, PAYMENTS_WITHOUT_ALIAS);
        List<IngestionResult> a = splitRun(ablated);

        report("B alias", "control", control, c);
        report("B alias", "ablated (no alias)", ablated, a);

        assertThat(control.pipeline.instances).hasSize(1);
        ReplayPipeline.Outcome joined = control.pipeline.outcome("split-shop::O-1");
        assertThat(joined.sagaStatus).isEqualTo("COMPLETED");
        assertThat(joined.rcaStatus).isEqualTo("NO_DIAGNOSIS_NEEDED");
        assertThat(control.pipeline.events).extracting(SagaEvent::getCorrelationBasis)
                .contains(CorrelationBasis.ALIAS_LINK);

        assertThat(ablated.pipeline.instances).as("one run observed as fragments").hasSize(2);
        ReplayPipeline.Outcome fragment = ablated.pipeline.outcome("split-shop::P-77");
        assertThat(fragment.primary + fragment.anomalies)
                .as("the payment fragment is diagnosed as something it is not")
                .isNotEqualTo(joined.primary + joined.anomalies);
    }

    private List<IngestionResult> splitRun(Arm arm) {
        List<IngestionResult> results = new ArrayList<>();
        results.add(arm.ingest("orders.events",
                "{\"id\":\"o1\",\"orderNo\":\"O-1\",\"type\":\"ORDER_CREATED\",\"at\":\"" + T0 + "\"}"));
        results.add(arm.ingest("payments.events",
                "{\"id\":\"p1\",\"paymentRef\":\"P-77\",\"orderNo\":\"O-1\",\"type\":\"PAYMENT_COMPLETED\",\"at\":\""
                        + T0.plusSeconds(2) + "\"}"));
        results.add(arm.ingest("orders.events",
                "{\"id\":\"o2\",\"orderNo\":\"O-1\",\"type\":\"INVENTORY_RESERVED\",\"at\":\"" + T0.plusSeconds(4) + "\"}"));
        return results;
    }

    @Test
    void aLateAliasFoldsTwoFragmentsAndWithdrawsWhatWasClaimedAgainstThem() {
        Arm control = new Arm(ORDERS, PAYMENTS_WITH_ALIAS);
        List<IngestionResult> c = foldRun(control);

        Arm noAlias = new Arm(ORDERS, PAYMENTS_WITHOUT_ALIAS);
        List<IngestionResult> n = foldRun(noAlias);

        Arm noWithdrawal = new Arm(ORDERS, PAYMENTS_WITH_ALIAS);
        noWithdrawal.pipeline.retractionEnabled = false;
        List<IngestionResult> w = foldRun(noWithdrawal);

        report("C fold", "control", control, c);
        report("C fold", "ablated (no alias)", noAlias, n);
        report("C fold", "ablated (no retract)", noWithdrawal, w);

        assertThat(control.pipeline.instances).as("folded into one run").hasSize(1);
        String survivor = control.pipeline.instances.keySet().iterator().next();
        ReplayPipeline.Outcome folded = control.pipeline.outcome(survivor);
        assertThat(folded.sagaStatus).isEqualTo("COMPLETED");
        assertThat(folded.retracted).as("the fragment's absence claim is withdrawn").isPositive();
        assertThat(folded.primary).isNull();

        assertThat(noAlias.pipeline.instances).hasSize(2);
        assertThat(noAlias.pipeline.outcome("split-shop::P-77").primary).isNotNull();

        assertThat(noWithdrawal.pipeline.instances).hasSize(1);
        ReplayPipeline.Outcome kept = noWithdrawal.pipeline.outcome(
                noWithdrawal.pipeline.instances.keySet().iterator().next());
        assertThat(kept.primary)
                .as("a complete run diagnosed from a claim made against one of its fragments")
                .isNotNull();
    }

    private List<IngestionResult> foldRun(Arm arm) {
        List<IngestionResult> results = new ArrayList<>();
        results.add(arm.ingest("orders.events",
                "{\"id\":\"o1\",\"orderNo\":\"O-1\",\"type\":\"ORDER_CREATED\",\"at\":\"" + T0 + "\"}"));
        results.add(arm.ingest("payments.events",
                "{\"id\":\"p0\",\"paymentRef\":\"P-77\",\"type\":\"PAYMENT_STARTED\",\"at\":\""
                        + T0.plusSeconds(1) + "\"}"));
        results.add(arm.ingest("payments.events",
                "{\"id\":\"p1\",\"paymentRef\":\"P-77\",\"orderNo\":\"O-1\",\"type\":\"PAYMENT_COMPLETED\",\"at\":\""
                        + T0.plusSeconds(2) + "\"}"));
        results.add(arm.ingest("orders.events",
                "{\"id\":\"o2\",\"orderNo\":\"O-1\",\"type\":\"INVENTORY_RESERVED\",\"at\":\"" + T0.plusSeconds(4) + "\"}"));
        return results;
    }

    private static final String TRACE_AND_KEY = """
            name: trace-shop
            topicPattern: 'trace-shop\\.events'
            fields:
              eventId: id
              executionId: orderNo
              eventType: type
              eventTime: at
              traceId: traceparentTraceId
            """;

    @Test
    void oneTraceSpanningTwoRunsIsTheLimitOfTheTraceRung() {
        String trace = "0af7651916cd43dd8448eb211c80319c";
        Arm traceOnly = new Arm(TRACE_ONLY);
        Arm withKey = new Arm(TRACE_AND_KEY);
        List<IngestionResult> t = new ArrayList<>(), k = new ArrayList<>();
        for (String run : List.of("A", "B")) {
            List<String> payloads = healthyRun(run, trace);
            for (String payload : payloads) {
                t.add(traceOnly.ingest("trace-shop.events", payload));
                k.add(withKey.ingest("trace-shop.events",
                        payload.replace("{", "{\"orderNo\":\"ORD-" + run + "\",")));
            }
        }
        report("D shared trace", "trace only", traceOnly, t);
        report("D shared trace", "declared key", withKey, k);

        assertThat(withKey.pipeline.instances).hasSize(2);
        assertThat(traceOnly.pipeline.instances).as("two runs filed as one").hasSize(1);
        ReplayPipeline.Outcome merged = traceOnly.pipeline.outcome(
                traceOnly.pipeline.instances.keySet().iterator().next());

        assertThat(merged.anomalies).containsExactly("OUT_OF_ORDER");
        assertThat(merged.primary).isNull();
    }

    private static List<String> healthyRun(String prefix, String traceId) {
        String[] types = {"ORDER_CREATED", "PAYMENT_COMPLETED", "INVENTORY_RESERVED"};
        List<String> run = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            run.add("{\"id\":\"" + prefix + i + "\",\"type\":\"" + types[i] + "\",\"at\":\""
                    + T0.plusSeconds(2L * i) + "\""
                    + (traceId != null ? ",\"traceparentTraceId\":\"" + traceId + "\"" : "") + "}");
        }
        return run;
    }

    private static void report(String experiment, String arm, Arm a, List<IngestionResult> results) {
        System.out.printf("[E3 %s] %-20s outcomes=%s executions=%s%n", experiment, arm,
                results.stream().map(r -> r.outcome().name()).toList(), a.pipeline.instances.keySet());
        for (String id : a.pipeline.instances.keySet()) {
            System.out.printf("[E3 %s] %-20s   %s -> %s%n", experiment, arm, id, a.pipeline.outcome(id));
        }
    }

    private static final class Arm {
        final ReplayPipeline pipeline = new ReplayPipeline();
        final Map<ExecutionAlias.Key, ExecutionAlias> aliasRows = new LinkedHashMap<>();
        final EventIngestionService ingestion;

        Arm(String... mappingYaml) {
            List<SourceMapping> mappings = new ArrayList<>();
            for (String yaml : mappingYaml) mappings.add(parse(yaml));
            SourceMappingRegistry registry = new SourceMappingRegistry() {
                @Override
                public SourceMapping forTopic(String topic) {
                    return mappings.stream().filter(m -> m.matches(topic)).findFirst()
                            .orElseGet(SourceMapping::identity);
                }
            };
            ExecutionAliasRepository aliases = mock(ExecutionAliasRepository.class);
            when(aliases.findById(any(ExecutionAlias.Key.class)))
                    .thenAnswer(c -> Optional.ofNullable(aliasRows.get(c.getArgument(0))));
            when(aliases.save(any(ExecutionAlias.class))).thenAnswer(c -> {
                ExecutionAlias alias = c.getArgument(0);
                aliasRows.put(new ExecutionAlias.Key(alias.getSourceName(), alias.getAliasKey()), alias);
                return alias;
            });
            when(aliases.findByExecutionId(anyString())).thenAnswer(c -> aliasRows.values().stream()
                    .filter(alias -> alias.getExecutionId().equals(c.getArgument(0))).toList());
            ExecutionAliasResolver resolver = new ExecutionAliasResolver(aliases, pipeline.eventRepo,
                    pipeline.sagas, pipeline.anomalyRepo, pipeline.rcaRepo);
            ingestion = new EventIngestionService(pipeline.correlation, new ObjectMapper(),
                    new SourceCompletenessCheck(), new DeduplicationCache(), new EventTimeResolver(),
                    registry, resolver, new InlineTransactionManager());
        }

        IngestionResult ingest(String topic, String payload) {
            return ingestion.ingest(new RawObservation(topic, payload, T0, null, null, Map.of()));
        }

        List<IngestionResult> ingestAll(String topic, List<String> payloads) {
            return payloads.stream().map(p -> ingest(topic, p)).toList();
        }
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
