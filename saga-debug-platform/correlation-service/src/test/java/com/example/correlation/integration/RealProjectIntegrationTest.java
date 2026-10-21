package com.example.correlation.integration;

import com.example.correlation.domain.*;
import com.example.correlation.service.ReplayHarness;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration with systems this project did not write.
 *
 * <p>Runs captured from four unmodified open-source saga implementations (retest 2026-10-11:
 * semotpan hotel 16d4f67, sylleryum KMS b474ba3, uuhnaut69 SPM 41da497, Eventuate e7ec5b7),
 * each judged with the declarations shipped in {@code declarations/}. The expected outcome of
 * every run is what the source system itself persisted, or the plan written before the fault
 * was injected -- never what the observer said (tools/build_integration_fixtures.py).
 *
 * <p>Each run is delivered in four orders, because an observer reading Kafka does not get to
 * choose. Checked: the reconstructed status matches the source; the findings left standing do
 * not depend on the order (OUT_OF_ORDER excepted, which is about the order); a run the
 * source settled normally carries no structural finding that would blame it; and a defect the
 * source is shown by its own state to have committed is reported.
 */
class RealProjectIntegrationTest {

    /** Claims that, standing on a run the source settled normally, blame it for something it did not do. */
    private static final Set<AnomalyType> STRUCTURAL = EnumSet.of(
            AnomalyType.FORBIDDEN_POST_TERMINAL, AnomalyType.STATE_REGRESSION,
            AnomalyType.MULTI_TERMINAL_CONFLICT, AnomalyType.INVALID_TRANSITION,
            AnomalyType.ORPHAN_EVENT, AnomalyType.MISSING_TERMINAL_EVENT);

    private static final Map<String, Function<List<Integer>, List<Integer>>> ORDERS = new LinkedHashMap<>();
    static {
        ORDERS.put("forward", l -> l);
        ORDERS.put("reverse", l -> { List<Integer> r = new ArrayList<>(l); Collections.reverse(r); return r; });
        ORDERS.put("last-first", l -> { List<Integer> r = new ArrayList<>(l); r.add(0, r.remove(r.size() - 1)); return r; });
        ORDERS.put("even-then-odd", l -> {
            List<Integer> r = new ArrayList<>();
            for (int i = 0; i < l.size(); i += 2) r.add(l.get(i));
            for (int i = 1; i < l.size(); i += 2) r.add(l.get(i));
            return r;
        });
    }

    private static JsonNode fixtures;
    private static WorkflowRegistry workflows;

    @BeforeAll
    static void load() throws Exception {
        try (InputStream in = RealProjectIntegrationTest.class.getResourceAsStream("/integration/real-project-fixtures.json")) {
            fixtures = new ObjectMapper().readTree(in);
        }
        workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", declarations().toString());
        workflows.load();
    }

    @Test
    void everyCapturedRunIsReadAsItsSourceSettledIt() {
        List<String> failures = new ArrayList<>();
        Map<String, int[]> perSystem = new TreeMap<>();   // system -> {runs, correct}
        List<Map<String, Object>> observed = new ArrayList<>();   // forward order, for baselines

        for (JsonNode fixture : fixtures) {
            String name = fixture.get("fixture").asText();
            String system = fixture.get("system").asText();
            String expected = fixture.get("expectedStatus").asText();
            List<Integer> natural = new ArrayList<>();
            for (int i = 0; i < fixture.get("events").size(); i++) natural.add(i);

            Set<String> standingAcrossOrders = new TreeSet<>();
            Map<String, List<String>> standingByOrder = new LinkedHashMap<>();
            for (Map.Entry<String, Function<List<Integer>, List<Integer>>> order : ORDERS.entrySet()) {
                ReplayHarness.Result run = ReplayHarness.replay(workflows, system, name,
                        fixture.get("workflow").asText(), events(fixture, order.getValue().apply(natural)));
                int[] tally = perSystem.computeIfAbsent(system, s -> new int[2]);
                tally[0]++;
                if (expected.equals(run.status())) {
                    tally[1]++;
                } else {
                    failures.add(name + " [" + order.getKey() + "]: status " + run.status()
                            + ", source settled " + expected + " (" + fixture.get("labelBasis").asText() + ")");
                }
                List<String> standing = run.standing().stream()
                        .filter(t -> t != AnomalyType.OUT_OF_ORDER).map(Enum::name).sorted().toList();
                standingByOrder.put(order.getKey(), standing);
                if ("forward".equals(order.getKey())) {
                    observed.add(Map.of("fixture", name, "system", system, "status", run.status(),
                            "standing", standing));
                }
                standingAcrossOrders.add(String.join(",", standing));
                Set<String> mustFind = new TreeSet<>();
                fixture.get("expectedFindings").forEach(f -> mustFind.add(f.asText()));
                for (String finding : mustFind) {
                    if (run.standing().stream().noneMatch(t -> t.name().equals(finding))) {
                        failures.add(name + " [" + order.getKey() + "]: missed " + finding + " -- "
                                + fixture.get("defectBasis").asText());
                    }
                }
                if (!"IN_PROGRESS".equals(expected)) {
                    run.standing().stream().filter(STRUCTURAL::contains)
                            .filter(t -> !mustFind.contains(t.name())).forEach(t ->
                            failures.add(name + " [" + order.getKey() + "]: " + t + " standing on a run the source settled " + expected));
                }
            }
            if (standingAcrossOrders.size() > 1) {
                failures.add(name + ": standing findings depend on delivery order " + standingByOrder);
            }
        }

        writeObserved(observed);
        perSystem.forEach((system, t) ->
                System.out.printf("REAL-PROJECT %-10s status correct %d/%d%n", system, t[1], t[0]));
        assertThat(failures).isEmpty();
    }

    /**
     * What the observer concluded per run, for the external baseline to be compared against
     * (tools/pm4py_real_baseline.py). Written to target/, never read back by a test.
     */
    private static void writeObserved(List<Map<String, Object>> observed) {
        try {
            Path out = Path.of("target", "real-project-observer.json");
            Files.createDirectories(out.getParent());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(out.toFile(), observed);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static List<ReplayHarness.CapturedEvent> events(JsonNode fixture, List<Integer> order) {
        List<ReplayHarness.CapturedEvent> events = new ArrayList<>();
        for (int index : order) {
            JsonNode e = fixture.get("events").get(index);
            events.add(new ReplayHarness.CapturedEvent(e.get("eventId").asText(), e.get("eventType").asText(),
                    Instant.parse(e.get("timestamp").asText()), e.get("serviceName").asText()));
        }
        return events;
    }

    private static Path declarations() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        return root.resolve("declarations");
    }
}
