package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.execution.EvidenceTimeline;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import com.example.correlation.workflow.WorkflowSpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * R25 — the run waited on a step longer than the step allows, and the container runtime
 * reported that the service owning that step was down during that wait.
 *
 * <h2>Why a separate source of evidence</h2>
 * A stopped service emits nothing, so from the saga's events a crash, a slow consumer and a
 * partitioned network are the same silence. R05 and R06 say exactly that and stop there:
 * R05 names SERVICE_CRASH only as a hypothesis, R06 only says the run went quiet and later
 * ended. What settles it is a fact from outside the events — the runtime saying the
 * process exited — which {@link ServiceLifecycleEvent} records. With it, the claim is no
 * longer inferred from absence.
 *
 * <h2>The two conditions, and why both</h2>
 * <ol>
 *   <li><b>A wait.</b> Between two consecutive observations of the run (or since the last
 *       one, while it has not ended) more time passed than the awaited step's declared
 *       timeout. Without this, any restart of a service would be blamed on every run that
 *       happened to be in flight.</li>
 *   <li><b>A down interval of that step's service overlapping the wait.</b> The service is
 *       the one the workflow declares for the step; the interval runs from a reported exit
 *       to the next reported start, or is still open. Without this, it is R05's silence
 *       again.</li>
 * </ol>
 *
 * <h2>Symptoms of the same wait</h2>
 * The step timeout (R07) and the silence-then-ending (R06) are true and are what the wait
 * looked like from the events. Once the cause of that wait is established they are its
 * consequences, not competing causes, so {@link #explainSymptoms} moves them to
 * SUPPORTING_EVIDENCE with the reason. They stay in the result with their evidence.
 */
@Component
// false = the events-only observer: R25 is not assessed at all, whatever the table holds.
// The measured ablation; turning off only the collector would leave older facts readable.
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        prefix = "correlation.liveness", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ServiceDownRule {

    public static final String RULE_ID = "R25";
    public static final String ROOT_CAUSE = "SERVICE_CRASH";

    private static final Set<SagaStatus> ENDED =
            Set.of(SagaStatus.COMPLETED, SagaStatus.COMPENSATED, SagaStatus.FAILED);

    private final ServiceLifecycleEventRepository repository;
    private final Duration lookback;

    public ServiceDownRule(ServiceLifecycleEventRepository repository,
            @Value("${correlation.liveness.lookback:PT1H}") Duration lookback) {
        this.repository = repository;
        this.lookback = lookback;
    }

    /** A stretch of the run spent waiting on one step for longer than the step allows. */
    public record Wait(String stepId, String service, Instant from, Instant to,
            int timeoutSeconds, String closingEventId, String closingEventType) {
        boolean isOpen() { return closingEventId == null; }
        long seconds() { return Duration.between(from, to).getSeconds(); }
    }

    /** What the rule concluded, with the wait it explained. */
    public record Finding(CandidateAssessment candidate, Wait explainedWait) {}

    public Optional<Finding> assess(SagaInstance saga, List<SagaEvent> events,
            WorkflowSpec workflow, Instant now) {
        List<Wait> waits = waits(saga, events, workflow, now);
        if (waits.isEmpty()) return Optional.empty();
        Set<String> services = new LinkedHashSet<>();
        waits.forEach(w -> services.add(w.service()));
        Instant from = waits.stream().map(Wait::from).min(Comparator.naturalOrder()).orElseThrow()
                .minus(lookback);
        Instant to = waits.stream().map(Wait::to).max(Comparator.naturalOrder()).orElseThrow();
        List<ServiceLifecycleEvent> lifecycle =
                repository.findByServiceNameInAndOccurredAtBetweenOrderByOccurredAtAsc(services, from, to);
        return assess(saga, waits, readable(lifecycle, workflow.getRuntimeScope()));
    }

    /**
     * The facts that can be attributed to this workflow's services (V21).
     *
     * <p>With a declared {@code runtimeScope}, only that deployment's facts. Without one, a
     * service name is usable only while a single scope reports it: when two deployments both
     * report a {@code payment-service}, neither restart can be pinned on this run, and the
     * rule makes no claim rather than picking one — as execution keys refuse an ambiguous raw
     * id (ADR 0002).
     */
    public static List<ServiceLifecycleEvent> readable(List<ServiceLifecycleEvent> lifecycle, String runtimeScope) {
        if (runtimeScope != null && !runtimeScope.isBlank()) {
            return lifecycle.stream().filter(e -> runtimeScope.equals(e.getScope())).toList();
        }
        Map<String, Set<Optional<String>>> scopesByService = new HashMap<>();
        lifecycle.forEach(e -> scopesByService.computeIfAbsent(e.getServiceName(), k -> new java.util.HashSet<>())
                .add(Optional.ofNullable(e.getScope())));
        return lifecycle.stream()
                .filter(e -> scopesByService.get(e.getServiceName()).size() == 1)
                .toList();
    }

    /**
     * Every wait in the run that exceeded its step's timeout, earliest first.
     *
     * <p>Measured on the platform's clock (receivedAt), as R05 measures silence (F15): the
     * runtime's event times come from the same host, and a source's own timestamps may lag.
     */
    static List<Wait> waits(SagaInstance saga, List<SagaEvent> events, WorkflowSpec workflow, Instant now) {
        if (workflow == null || events == null || events.isEmpty()) return List.of();
        // Business order decides which step a silence belongs to (I9): events of one run
        // travel on several topics and are read in whatever order the partitions deliver,
        // so the event read just before a silence need not be the one that preceded it.
        // The silence itself is still measured on the platform's clock, from the last
        // moment anything about the run had been heard.
        List<SagaEvent> heard = EvidenceTimeline.of(workflow, events.stream()
                        .filter(e -> heardAt(e) != null).toList())
                .inBusinessOrder();
        List<Wait> waits = new ArrayList<>();
        if (heard.isEmpty()) return waits;

        Instant lastHeard = heardAt(heard.get(0));
        for (int i = 1; i < heard.size(); i++) {
            SagaEvent previous = heard.get(i - 1);
            SagaEvent current = heard.get(i);
            Instant from = lastHeard;
            Instant to = heardAt(current);
            if (to.isAfter(lastHeard)) lastHeard = to;
            for (String step : awaitedBetween(workflow, previous, current)) {
                Optional<String> service = workflow.serviceOf(step);
                if (service.isEmpty()) continue;
                int timeout = workflow.getStepTimeoutSeconds(step);
                if (Duration.between(from, to).getSeconds() > timeout) {
                    waits.add(new Wait(step, service.get(), from, to, timeout,
                            current.getEventId(), current.getEventType()));
                }
            }
        }

        if (saga.getStatus() != null && !ENDED.contains(saga.getStatus())) {
            SagaEvent last = heard.get(heard.size() - 1);
            Instant from = lastHeard;
            workflow.classifyEvent(last.getEventType()).ifPresent(role -> {
                List<String> awaited = switch (role.role()) {
                    case COMPLETED -> workflow.stepsAfter(role.stepId());
                    case FAILED -> List.of();
                    default -> List.of(role.stepId());
                };
                for (String step : awaited) {
                    Optional<String> service = workflow.serviceOf(step);
                    if (service.isEmpty()) continue;
                    int timeout = workflow.getStepTimeoutSeconds(step);
                    if (Duration.between(from, now).getSeconds() > timeout) {
                        waits.add(new Wait(step, service.get(), from, now, timeout, null, null));
                    }
                }
            });
        }
        waits.sort(Comparator.comparing(Wait::from));
        return waits;
    }

    /**
     * The step(s) the run was waiting on between two observations: those awaited after the
     * earlier one (EXT-R25-MISS-KITCHEN), the same reading the open wait below and the stuck
     * timeout (EXT-STUCK-TIMEOUT) use. The closing event's own step is the answer only when
     * the earlier event says nothing about what comes next — it belongs to no step, or ends a
     * branch — which is how every wait was read before.
     *
     * <p>Charging the wait to the closing event's step was right whenever each step publishes
     * its own completion: the reply that ends the wait belongs to the awaited step. A source
     * that does not — an orchestrator seen through its commands, an untranslated completion —
     * ends the wait with a later step's event, and the service that held the run up went
     * unnamed.
     */
    private static List<String> awaitedBetween(WorkflowSpec workflow, SagaEvent previous, SagaEvent current) {
        List<String> awaited = workflow.stepsAwaitedAfter(previous.getEventType());
        if (!awaited.isEmpty()) return awaited;
        return workflow.classifyEvent(current.getEventType())
                .map(WorkflowSpec.EventRole::stepId)
                .map(List::of)
                .orElse(List.of());
    }

    /**
     * The first wait some down interval of its service overlaps, preferring an interval the
     * runtime reported (PROCESS) over one a probe inferred (PROBE): the first settles that the
     * process was gone, the second only that it could not be reached.
     */
    static Optional<Finding> assess(SagaInstance saga, List<Wait> waits,
            List<ServiceLifecycleEvent> lifecycle) {
        Map<String, List<DownInterval>> downByService = downIntervals(lifecycle);
        Finding probed = null;
        for (Wait wait : waits) {
            for (DownInterval down : downByService.getOrDefault(wait.service(), List.of())) {
                if (!down.overlaps(wait.from(), wait.to())) continue;
                if (down.exit().getBasis() == ServiceLifecycleEvent.Basis.PROCESS) {
                    return Optional.of(new Finding(candidate(saga, wait, down), wait));
                }
                if (probed == null) probed = new Finding(candidate(saga, wait, down), wait);
            }
        }
        return Optional.ofNullable(probed);
    }

    private record DownInterval(ServiceLifecycleEvent exit, ServiceLifecycleEvent start) {
        Instant from() { return exit.getOccurredAt(); }
        Instant to() { return start != null ? start.getOccurredAt() : null; }
        boolean overlaps(Instant waitFrom, Instant waitTo) {
            return from().isBefore(waitTo) && (to() == null || to().isAfter(waitFrom));
        }
    }

    /**
     * Exit-to-start intervals per service. A start with no exit before it in the window
     * opens nothing: the service may have been down before the window, but that was not
     * observed, and an interval with an invented beginning is not evidence.
     */
    private static Map<String, List<DownInterval>> downIntervals(List<ServiceLifecycleEvent> lifecycle) {
        Map<String, List<DownInterval>> result = new HashMap<>();
        Map<String, ServiceLifecycleEvent> openExit = new HashMap<>();
        lifecycle.stream()
                .sorted(Comparator.comparing(ServiceLifecycleEvent::getOccurredAt))
                .forEach(event -> {
                    String service = event.getServiceName();
                    if (event.getState() == ServiceLifecycleEvent.State.DOWN) {
                        openExit.putIfAbsent(service, event);
                    } else {
                        ServiceLifecycleEvent exit = openExit.remove(service);
                        if (exit != null) {
                            result.computeIfAbsent(service, s -> new ArrayList<>())
                                    .add(new DownInterval(exit, event));
                        }
                    }
                });
        openExit.forEach((service, exit) -> result.computeIfAbsent(service, s -> new ArrayList<>())
                .add(new DownInterval(exit, null)));
        return result;
    }

    private static CandidateAssessment candidate(SagaInstance saga, Wait wait, DownInterval down) {
        boolean ended = saga.getStatus() != null && ENDED.contains(saga.getStatus());
        CandidateAssessment c = CandidateAssessment.builder()
                .ruleId(RULE_ID)
                .rootCause(ROOT_CAUSE)
                .severity(ended ? "MEDIUM" : "CRITICAL")
                .status(CandidateStatus.STRONG_EVIDENCE)
                .build();

        ServiceLifecycleEvent exit = down.exit();
        c.getEvidence().add(Evidence.of(
                wait.service() + (exit.getBasis() == ServiceLifecycleEvent.Basis.PROBE
                                ? " stopped answering probes at " : " exited at ") + exit.getOccurredAt()
                        + " (" + exit.getDetail() + ", reported by " + exit.getSource() + ")",
                exit.getDetail(), "running while " + wait.stepId() + " is awaited", null, List.of()));
        if (down.start() != null) {
            long downFor = Duration.between(exit.getOccurredAt(), down.start().getOccurredAt()).getSeconds();
            c.getEvidence().add(Evidence.of(
                    wait.service() + " was started again at " + down.start().getOccurredAt()
                            + " after " + downFor + "s down", null, null, null, List.of()));
        } else {
            c.getEvidence().add(Evidence.of(
                    wait.service() + " has not been reported started again", null, null, null, List.of()));
        }
        String waited = "Run waited " + wait.seconds() + "s for step " + wait.stepId()
                + " (declared timeout " + wait.timeoutSeconds() + "s)";
        if (wait.isOpen()) {
            c.getEvidence().add(Evidence.of(waited + " and nothing for it has arrived",
                    wait.seconds() + "s", "<= " + wait.timeoutSeconds() + "s", null, List.of()));
        } else {
            c.getEvidence().add(Evidence.ofEvent(waited + " until " + wait.closingEventType() + " arrived",
                    wait.seconds() + "s", "<= " + wait.timeoutSeconds() + "s", wait.closingEventId()));
        }

        c.setAnchorStepId(wait.stepId());
        if (exit.getBasis() == ServiceLifecycleEvent.Basis.PROBE) {
            // A probe that could not reach the service is also what a partition looks like,
            // so this corroborates a crash without settling it.
            c.setStatus(CandidateStatus.SUPPORTING_EVIDENCE);
            c.setStatusReason(wait.service() + " could not be reached by a probe while the run was "
                    + "waiting on " + wait.stepId() + "; a probe cannot tell a stopped process "
                    + "from an unreachable one.");
        } else {
            c.setStatusReason("The runtime reported " + wait.service()
                    + "'s process exited while the run was waiting on " + wait.stepId()
                    + "; the run's own events could only show the silence.");
        }
        c.getSuggestions().add("Check " + wait.service() + " logs before " + exit.getOccurredAt()
                + " for why it exited (" + exit.getDetail() + ")");
        c.getSuggestions().add("Review " + wait.service() + "'s restart policy and health checks");
        return c;
    }

    /**
     * Moves the findings that only describe this wait from competing cause to consequence.
     *
     * <p>Two, and only when they are about this wait: a step timeout (R07) whose cited
     * STEP_TIMEOUT is for the same step, and the silence-then-ending (R06) when every
     * SAGA_STUCK on the run was raised inside the wait. Anything else — another step's
     * timeout, a failure the run reported itself — is left as it was.
     */
    public static void explainSymptoms(List<CandidateAssessment> candidates, List<Anomaly> anomalies,
            Finding finding) {
        // Only an established cause turns other findings into its consequences.
        if (finding.candidate().getStatus() != CandidateStatus.STRONG_EVIDENCE) return;
        Wait wait = finding.explainedWait();
        Map<Long, Anomaly> byId = new HashMap<>();
        anomalies.stream().filter(a -> a.getId() != null).forEach(a -> byId.put(a.getId(), a));
        String reason = "Consequence of " + RULE_ID + ": " + wait.service()
                + " was down while the run waited on " + wait.stepId() + ".";

        for (CandidateAssessment c : candidates) {
            if (c.getStatus() != CandidateStatus.STRONG_EVIDENCE) continue;
            boolean symptom = false;
            if ("R07".equals(c.getRuleId())) {
                List<Anomaly> cited = c.getEvidence().stream()
                        .map(Evidence::anomalyId).filter(Objects::nonNull)
                        .map(byId::get).filter(Objects::nonNull).toList();
                symptom = !cited.isEmpty() && cited.stream().allMatch(a ->
                        a.getAnomalyType() == AnomalyType.STEP_TIMEOUT
                                && ("workflow:" + wait.stepId() + ".timeout").equals(a.getThresholdSource()));
            } else if ("R06".equals(c.getRuleId())) {
                List<Anomaly> stuck = anomalies.stream()
                        .filter(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK)
                        .filter(a -> a.getDetectedAt() != null).toList();
                symptom = !stuck.isEmpty() && stuck.stream().allMatch(a ->
                        !a.getDetectedAt().isBefore(wait.from()) && !a.getDetectedAt().isAfter(wait.to()));
            }
            if (symptom) {
                c.setStatus(CandidateStatus.SUPPORTING_EVIDENCE);
                c.setStatusReason(reason);
            }
        }
    }

    private static Instant heardAt(SagaEvent event) {
        return event.getReceivedAt() != null ? event.getReceivedAt() : event.getTimestamp();
    }
}
