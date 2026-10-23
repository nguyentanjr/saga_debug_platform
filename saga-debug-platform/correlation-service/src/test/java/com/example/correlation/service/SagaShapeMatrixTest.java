package com.example.correlation.service;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpec.StepSpec;
import com.example.correlation.workflow.WorkflowSpec.TransitionSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Systematic coverage of saga shapes, instead of one observed system at a time.
 *
 * <p>Each real system so far exercised a few shapes; the observer was fixed for those and
 * stayed wrong on the ones no system had yet produced (ShopSaga, 2026-10-22: two
 * compensation branches at once, a cancel mid-step, a reply arriving after compensation
 * began). The shapes are finite, so they are enumerated here:
 *
 * <ul>
 *   <li>step structure: sequential (2..4 steps), parallel fan-out with join;</li>
 *   <li>compensation structure: none, one branch, every completed step;</li>
 *   <li>incident: none, business failure at each step, cancel mid-step, reply after cancel;</li>
 *   <li>compensation mode: concurrent branches, sequential in reverse;</li>
 *   <li>transport: in order, reversed, shuffled, one event delivered twice.</li>
 * </ul>
 *
 * <p>Every generated run is legitimate behaviour of the observed system, so two invariants
 * must hold: the status equals the outcome the generator produced, and no finding against
 * the observed system stands. Findings about the transport itself (arrival order, a
 * redelivery, absent trace context) are allowed where the transport variant produced them.
 */
class SagaShapeMatrixTest {

    private static final Instant T0 = Instant.parse("2026-10-22T00:00:00Z");
    private static final Set<String> ALWAYS_ALLOWED = Set.of("MISSING_TRACE_CONTEXT");

    enum Structure { SEQ2, SEQ3, SEQ4, PARALLEL }
    enum Compensation { NONE, FIRST_ONLY, ALL }
    enum Mode { CONCURRENT, REVERSE }
    enum Transport { IN_ORDER, REVERSED, SHUFFLED_1, SHUFFLED_2, SHUFFLED_3, DUPLICATE }

    record Ev(String id, String type, Instant at) {}

    /** @param required findings the run must raise: true positives, not false ones */
    record Run(String scenario, List<Ev> events, String expected, Set<String> required) {
        Run(String scenario, List<Ev> events, String expected) { this(scenario, events, expected, Set.of()); }
    }

    @Test
    void everyGeneratedShapeEndsAsGeneratedWithoutFalseFindings() {
        List<String> failures = new ArrayList<>();
        Map<String, int[]> perScenario = new TreeMap<>();
        int cells = 0;
        for (Structure structure : Structure.values()) {
            for (Compensation compensation : Compensation.values()) {
                WorkflowSpec spec = workflow(structure, compensation);
                WorkflowRegistry registry = WorkflowSpecTestFixture.registryOf(spec);
                for (Mode mode : Mode.values()) {
                    for (Run run : runs(structure, compensation, mode)) {
                        for (Transport transport : Transport.values()) {
                            cells++;
                            String cell = structure + "/" + compensation + "/" + run.scenario() + "/" + mode + "/" + transport;
                            String problem = check(registry, spec.getName(), run, transport, cells);
                            int[] tally = perScenario.computeIfAbsent(run.scenario().replaceAll("@.*", ""), k -> new int[2]);
                            tally[0]++;
                            if (problem != null) { tally[1]++; failures.add(cell + " -> " + problem); }
                        }
                    }
                }
            }
        }
        StringBuilder report = new StringBuilder("cells=" + cells + " failing=" + failures.size() + "\n");
        perScenario.forEach((k, v) -> report.append(String.format("  %-14s %4d/%4d failing%n", k, v[1], v[0])));
        // One line per distinct symptom, with a count and the first cell that shows it.
        Map<String, List<String>> bySymptom = new TreeMap<>();
        failures.forEach(f -> bySymptom.computeIfAbsent(f.substring(f.indexOf(" -> ") + 4), k -> new ArrayList<>()).add(f));
        bySymptom.forEach((symptom, hit) -> report.append(String.format("  %4d x %s  e.g. %s%n",
                hit.size(), symptom, hit.get(0).substring(0, hit.get(0).indexOf(" -> ")))));
        System.out.println(report);
        assertThat(failures).as(report.toString()).isEmpty();
    }

    private static String check(WorkflowRegistry registry, String workflow, Run run, Transport transport, int seed) {
        List<Ev> delivery = new ArrayList<>(run.events());
        switch (transport) {
            case IN_ORDER -> { }
            case REVERSED -> Collections.reverse(delivery);
            case SHUFFLED_1, SHUFFLED_2, SHUFFLED_3 -> Collections.shuffle(delivery, new Random(seed));
            case DUPLICATE -> delivery.add(new Random(seed).nextInt(delivery.size() + 1),
                    delivery.get(new Random(seed + 1).nextInt(delivery.size())));
        }
        ReplayPipeline p = new ReplayPipeline(registry);
        ExecutionKey key = ExecutionKey.of("gen", "run-" + seed);
        Instant arrival = T0.plusSeconds(3600);
        for (Ev e : delivery) {
            arrival = arrival.plusMillis(10);
            p.deliver(List.of(new IngestedEvent(
                    new IngestedEvent.Identity("gen", key, e.id(), null, CorrelationBasis.DECLARED_KEY),
                    workflow, e.type(), null, "svc", e.at(), "{}", arrival,
                    IngestedEvent.Causality.NONE, null)));
        }
        ReplayPipeline.Outcome o = p.outcome(key.canonical());
        Set<String> allowed = new HashSet<>(ALWAYS_ALLOWED);
        allowed.addAll(run.required());
        if (!o.anomalies.containsAll(run.required())) {
            return "missed " + run.required() + " standing=" + o.anomalies;
        }
        if (transport != Transport.IN_ORDER && transport != Transport.DUPLICATE) allowed.add("OUT_OF_ORDER");
        if (transport == Transport.DUPLICATE) { allowed.add("EXACT_DUPLICATE_EVENT"); allowed.add("OUT_OF_ORDER"); }
        List<String> falseFindings = o.anomalies.stream().filter(a -> !allowed.contains(a)).toList();
        if (!o.sagaStatus.equals(run.expected()) || !falseFindings.isEmpty()) {
            return "status=" + o.sagaStatus + " expected=" + run.expected() + " false=" + falseFindings;
        }
        // A run undone without any step reporting failure: no step failure may be diagnosed.
        boolean noStepFailed = run.events().stream().noneMatch(e -> e.type().endsWith("_FAIL"));
        if (noStepFailed && o.candidateRules.stream().anyMatch(r -> r.startsWith("R11:"))) {
            return "diagnosed FORWARD_STEP_FAILURE with no step failure observed";
        }
        return null;
    }

    // ── workflow shapes ───────────────────────────────────────────────────────

    private static List<String> steps(Structure s) {
        return switch (s) {
            case SEQ2 -> List.of("S1", "S2");
            case SEQ3 -> List.of("S1", "S2", "S3");
            case SEQ4, PARALLEL -> List.of("S1", "S2", "S3", "S4");
        };
    }

    /** Direct predecessors of each step. PARALLEL: S1 -> {S2, S3} -> S4. */
    private static Map<String, List<String>> predecessors(Structure s) {
        Map<String, List<String>> pre = new LinkedHashMap<>();
        List<String> steps = steps(s);
        if (s == Structure.PARALLEL) {
            pre.put("S1", List.of());
            pre.put("S2", List.of("S1"));
            pre.put("S3", List.of("S1"));
            pre.put("S4", List.of("S2", "S3"));
        } else {
            for (int i = 0; i < steps.size(); i++) pre.put(steps.get(i), i == 0 ? List.of() : List.of(steps.get(i - 1)));
        }
        return pre;
    }

    /** Every step that must complete before this one. */
    private static Set<String> upstream(Structure structure, String step) {
        Set<String> all = new LinkedHashSet<>();
        for (String p : predecessors(structure).get(step)) { all.add(p); all.addAll(upstream(structure, p)); }
        return all;
    }

    private static boolean compensable(Compensation c, String step) {
        return c == Compensation.ALL || (c == Compensation.FIRST_ONLY && step.equals("S1"));
    }

    private static WorkflowSpec workflow(Structure structure, Compensation compensation) {
        WorkflowSpec spec = new WorkflowSpec();
        spec.setName("GEN_" + structure + "_" + compensation);
        spec.setVersion("1");
        List<StepSpec> forward = new ArrayList<>();
        List<StepSpec> backward = new ArrayList<>();
        List<TransitionSpec> transitions = new ArrayList<>();
        Map<String, List<String>> pre = predecessors(structure);
        List<String> steps = steps(structure);
        for (String id : steps) {
            StepSpec step = step(id, Map.of("started", id + "_REQ", "completed", id + "_DONE", "failed", id + "_FAIL"));
            if (compensable(compensation, id)) {
                step.setCompensation("C" + id);
                backward.add(step("C" + id, Map.of("started", "C" + id + "_REQ", "completed", "C" + id + "_DONE")));
                transitions.add(transition("C" + id, "COMPENSATED", "SUCCESS"));
            }
            forward.add(step);
            if (pre.get(id).isEmpty()) transitions.add(transition("STARTED", id, null));
            for (String p : pre.get(id)) transitions.add(transition(p, id, "SUCCESS"));
            boolean undoable = upstream(structure, id).stream().anyMatch(s -> compensable(compensation, s));
            transitions.add(transition(id, undoable ? "COMPENSATING" : "FAILED", "FAILED"));
        }
        transitions.add(transition(steps.get(steps.size() - 1), "COMPLETED", "SUCCESS"));
        spec.setSteps(forward);
        spec.setCompensationSteps(backward);
        spec.setTransitions(transitions);
        return spec;
    }

    private static StepSpec step(String id, Map<String, String> events) {
        StepSpec s = new StepSpec();
        s.setId(id);
        s.setService("svc-" + id.toLowerCase());
        s.setTimeout("30s");
        s.setEvents(new LinkedHashMap<>(events));
        return s;
    }

    private static TransitionSpec transition(String from, String to, String condition) {
        TransitionSpec t = new TransitionSpec();
        t.setFrom(from);
        t.setTo(to);
        t.setCondition(condition);
        return t;
    }

    // ── incidents ─────────────────────────────────────────────────────────────

    /** Builds one run's events with strictly increasing business time. */
    private static final class Script {
        final List<Ev> events = new ArrayList<>();
        int second;
        void emit(String type) { events.add(new Ev("e" + events.size(), type, T0.plusSeconds(second++))); }
    }

    private static List<Run> runs(Structure structure, Compensation compensation, Mode mode) {
        List<Run> runs = new ArrayList<>();
        List<String> steps = steps(structure);

        // Normal run. PARALLEL interleaves the two arms.
        Script ok = new Script();
        forwardUntil(ok, structure, steps.size(), null);
        runs.add(new Run("normal", ok.events, "COMPLETED"));

        for (int k = 0; k < steps.size(); k++) {
            String failing = steps.get(k);

            // Business failure at step k: everything upstream completed, compensated.
            Script fail = new Script();
            List<String> done = forwardUntil(fail, structure, k, failing);
            fail.emit(failing + "_REQ");
            fail.emit(failing + "_FAIL");
            List<String> toUndo = done.stream().filter(s -> compensable(compensation, s)).toList();
            compensate(fail, toUndo, mode);
            runs.add(new Run("fail@" + failing, fail.events, toUndo.isEmpty() ? "FAILED" : "COMPENSATED"));

            // Cancelled while step k is in flight: completed steps and k itself are undone.
            List<String> cancelUndo = new ArrayList<>(done.stream().filter(s -> compensable(compensation, s)).toList());
            if (compensable(compensation, failing)) cancelUndo.add(failing);
            if (cancelUndo.isEmpty()) continue;
            Script cancel = new Script();
            forwardUntil(cancel, structure, k, failing);
            cancel.emit(failing + "_REQ");
            compensate(cancel, cancelUndo, mode);
            runs.add(new Run("cancel@" + failing, cancel.events, "COMPENSATED"));

            // Same, and the in-flight step's reply arrives after compensation began.
            Script late = new Script();
            forwardUntil(late, structure, k, failing);
            late.emit(failing + "_REQ");
            List<Ev> before = new ArrayList<>(late.events);
            compensate(late, cancelUndo, mode);
            // The reply lands after each compensation observation in turn, while one is still
            // open: after a request, and after one branch finished (ShopSaga customer-cancel).
            for (int at = before.size(); at < late.events.size() - 1; at++) {
                Ev reply = new Ev("e-late", failing + "_DONE", late.events.get(at).at().plusMillis(500));
                List<Ev> withReply = new ArrayList<>(late.events);
                withReply.add(at + 1, reply);
                runs.add(new Run("lateReply@" + failing + "+" + (at - before.size()), withReply, "COMPENSATED"));
            }

            // The source re-sends the in-flight step's compensation once its declared deadline
            // (30s) passed unanswered, and both requests are answered (ShopSaga payment-stop:
            // one refund row, two replies). A retry, not a duplicate.
            // Re-sent well inside the deadline, it is a duplicate (SC23) and must be reported.
            if (compensable(compensation, failing)) {
                String undo = "C" + failing;
                int reqAt = 0;
                while (!cancel.events.get(reqAt).type().equals(undo + "_REQ")) reqAt++;
                Instant firstRequest = cancel.events.get(reqAt).at();
                List<Ev> retried = new ArrayList<>();
                for (Ev e : cancel.events) {
                    boolean afterRequest = e.at().isAfter(firstRequest);
                    retried.add(afterRequest ? new Ev(e.id(), e.type(), e.at().plusSeconds(32)) : e);
                }
                int doneAt = 0;
                while (!retried.get(doneAt).type().equals(undo + "_DONE")) doneAt++;
                Ev answered = retried.get(doneAt);
                retried.add(doneAt, new Ev("e-retry", undo + "_REQ", firstRequest.plusSeconds(31)));
                retried.add(doneAt + 2, new Ev("e-reack", undo + "_DONE", answered.at().plusMillis(200)));
                retried.sort(Comparator.comparing(Ev::at));
                runs.add(new Run("retriedUndo@" + failing, retried, "COMPENSATED"));

                List<Ev> duplicated = new ArrayList<>(cancel.events);
                duplicated.add(reqAt + 1, new Ev("e-dup", undo + "_REQ", firstRequest.plusMillis(500)));
                runs.add(new Run("duplicateUndo@" + failing, duplicated, "COMPENSATED",
                        Set.of("DUPLICATE_COMPENSATION")));
            }
        }
        return runs;
    }

    /**
     * Completes the forward steps before position {@code k} (all of them when k = size).
     * In PARALLEL, a failing arm's sibling still completes: it owed nothing to the failure.
     *
     * @return the steps completed
     */
    private static List<String> forwardUntil(Script s, Structure structure, int k, String failing) {
        List<String> steps = steps(structure);
        List<String> done = new ArrayList<>();
        if (structure != Structure.PARALLEL) {
            for (int i = 0; i < k; i++) { s.emit(steps.get(i) + "_REQ"); s.emit(steps.get(i) + "_DONE"); done.add(steps.get(i)); }
            return done;
        }
        if (k >= 1) { s.emit("S1_REQ"); s.emit("S1_DONE"); done.add("S1"); }
        if (k == 0) return done;
        if (k == 1 || k == 2) {
            // Failing step is S2 or S3; the other arm starts first and completes.
            String sibling = "S2".equals(failing) ? "S3" : "S2";
            s.emit(sibling + "_REQ");
            s.emit(sibling + "_DONE");
            done.add(sibling);
            return done;
        }
        s.emit("S2_REQ"); s.emit("S3_REQ"); s.emit("S3_DONE"); s.emit("S2_DONE");
        done.add("S2"); done.add("S3");
        if (k >= 4) { s.emit("S4_REQ"); s.emit("S4_DONE"); done.add("S4"); }
        return done;
    }

    private static void compensate(Script s, List<String> steps, Mode mode) {
        List<String> order = new ArrayList<>(steps);
        Collections.reverse(order);
        if (mode == Mode.CONCURRENT) {
            order.forEach(step -> s.emit("C" + step + "_REQ"));
            order.forEach(step -> s.emit("C" + step + "_DONE"));
        } else {
            order.forEach(step -> { s.emit("C" + step + "_REQ"); s.emit("C" + step + "_DONE"); });
        }
    }
}
