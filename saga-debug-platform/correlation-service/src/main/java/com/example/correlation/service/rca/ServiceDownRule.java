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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Component
@ConditionalOnProperty(
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

    public record Wait(String stepId, String service, Instant from, Instant to,
            int timeoutSeconds, String closingEventId, String closingEventType) {
        boolean isOpen() { return closingEventId == null; }
        long seconds() { return Duration.between(from, to).getSeconds(); }
    }

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

    public static List<ServiceLifecycleEvent> readable(List<ServiceLifecycleEvent> lifecycle, String runtimeScope) {
        if (runtimeScope != null && !runtimeScope.isBlank()) {
            return lifecycle.stream().filter(e -> runtimeScope.equals(e.getScope())).toList();
        }
        Map<String, Set<Optional<String>>> scopesByService = new HashMap<>();
        lifecycle.forEach(e -> scopesByService.computeIfAbsent(e.getServiceName(), k -> new HashSet<>())
                .add(Optional.ofNullable(e.getScope())));
        return lifecycle.stream()
                .filter(e -> scopesByService.get(e.getServiceName()).size() == 1)
                .toList();
    }

    static List<Wait> waits(SagaInstance saga, List<SagaEvent> events, WorkflowSpec workflow, Instant now) {
        if (workflow == null || events == null || events.isEmpty()) return List.of();

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
            addOverdueWaits(waits, workflow, awaitedBetween(workflow, previous, current), from, to, current);
        }

        if (saga.getStatus() != null && !ENDED.contains(saga.getStatus())) {
            SagaEvent last = heard.get(heard.size() - 1);
            Optional<WorkflowSpec.EventRole> lastRole = workflow.classifyEvent(last.getEventType());
            if (lastRole.isPresent()) {
                addOverdueWaits(waits, workflow, awaitedAfter(workflow, lastRole.get()), lastHeard, now, null);
            }
        }
        waits.sort(Comparator.comparing(Wait::from));
        return waits;
    }

    private static List<String> awaitedAfter(WorkflowSpec workflow, WorkflowSpec.EventRole role) {
        return switch (role.role()) {
            case COMPLETED -> workflow.stepsAfter(role.stepId());
            case FAILED -> List.of();
            default -> List.of(role.stepId());
        };
    }

    private static void addOverdueWaits(List<Wait> waits, WorkflowSpec workflow, List<String> steps,
            Instant from, Instant to, SagaEvent closingEvent) {
        for (String step : steps) {
            Optional<String> service = workflow.serviceOf(step);
            if (service.isEmpty()) continue;
            int timeout = workflow.getStepTimeoutSeconds(step);
            if (Duration.between(from, to).getSeconds() <= timeout) continue;
            waits.add(new Wait(step, service.get(), from, to, timeout,
                    closingEvent != null ? closingEvent.getEventId() : null,
                    closingEvent != null ? closingEvent.getEventType() : null));
        }
    }

    private static List<String> awaitedBetween(WorkflowSpec workflow, SagaEvent previous, SagaEvent current) {
        List<String> awaited = workflow.stepsAwaitedAfter(previous.getEventType());
        if (!awaited.isEmpty()) return awaited;
        return workflow.classifyEvent(current.getEventType())
                .map(WorkflowSpec.EventRole::stepId)
                .map(List::of)
                .orElse(List.of());
    }

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
        CandidateAssessment candidate = CandidateAssessment.builder()
                .ruleId(RULE_ID)
                .rootCause(ROOT_CAUSE)
                .severity(ended ? "MEDIUM" : "CRITICAL")
                .status(CandidateStatus.STRONG_EVIDENCE)
                .build();

        ServiceLifecycleEvent exit = down.exit();
        candidate.getEvidence().add(Evidence.of(
                wait.service() + (exit.getBasis() == ServiceLifecycleEvent.Basis.PROBE
                                ? " stopped answering probes at " : " exited at ") + exit.getOccurredAt()
                        + " (" + exit.getDetail() + ", reported by " + exit.getSource() + ")",
                exit.getDetail(), "running while " + wait.stepId() + " is awaited", null, List.of()));
        if (down.start() != null) {
            long downFor = Duration.between(exit.getOccurredAt(), down.start().getOccurredAt()).getSeconds();
            candidate.getEvidence().add(Evidence.of(
                    wait.service() + " was started again at " + down.start().getOccurredAt()
                            + " after " + downFor + "s down", null, null, null, List.of()));
        } else {
            candidate.getEvidence().add(Evidence.of(
                    wait.service() + " has not been reported started again", null, null, null, List.of()));
        }
        String waited = "Run waited " + wait.seconds() + "s for step " + wait.stepId()
                + " (declared timeout " + wait.timeoutSeconds() + "s)";
        if (wait.isOpen()) {
            candidate.getEvidence().add(Evidence.of(waited + " and nothing for it has arrived",
                    wait.seconds() + "s", "<= " + wait.timeoutSeconds() + "s", null, List.of()));
        } else {
            candidate.getEvidence().add(Evidence.ofEvent(waited + " until " + wait.closingEventType() + " arrived",
                    wait.seconds() + "s", "<= " + wait.timeoutSeconds() + "s", wait.closingEventId()));
        }

        candidate.setAnchorStepId(wait.stepId());
        if (exit.getBasis() == ServiceLifecycleEvent.Basis.PROBE) {
            candidate.setStatus(CandidateStatus.SUPPORTING_EVIDENCE);
            candidate.setStatusReason(wait.service() + " could not be reached by a probe while the run was "
                    + "waiting on " + wait.stepId() + "; a probe cannot tell a stopped process "
                    + "from an unreachable one.");
        } else {
            candidate.setStatusReason("The runtime reported " + wait.service()
                    + "'s process exited while the run was waiting on " + wait.stepId()
                    + "; the run's own events could only show the silence.");
        }
        candidate.getSuggestions().add("Check " + wait.service() + " logs before " + exit.getOccurredAt()
                + " for why it exited (" + exit.getDetail() + ")");
        candidate.getSuggestions().add("Review " + wait.service() + "'s restart policy and health checks");
        return candidate;
    }

    public static void explainSymptoms(List<CandidateAssessment> candidates, List<Anomaly> anomalies,
            Finding finding) {
        if (finding.candidate().getStatus() != CandidateStatus.STRONG_EVIDENCE) return;
        Wait wait = finding.explainedWait();
        Map<Long, Anomaly> byId = new HashMap<>();
        anomalies.stream().filter(a -> a.getId() != null).forEach(a -> byId.put(a.getId(), a));
        String reason = "Consequence of " + RULE_ID + ": " + wait.service()
                + " was down while the run waited on " + wait.stepId() + ".";

        for (CandidateAssessment candidate : candidates) {
            if (candidate.getStatus() != CandidateStatus.STRONG_EVIDENCE) continue;
            boolean symptom = switch (String.valueOf(candidate.getRuleId())) {
                case "R07" -> citesOnlyTimeoutOf(candidate, wait, byId);
                case "R06" -> allStuckFindingsWithin(anomalies, wait);
                default -> false;
            };
            if (symptom) {
                candidate.setStatus(CandidateStatus.SUPPORTING_EVIDENCE);
                candidate.setStatusReason(reason);
            }
        }
    }

    private static boolean citesOnlyTimeoutOf(CandidateAssessment candidate, Wait wait, Map<Long, Anomaly> byId) {
        List<Anomaly> cited = candidate.getEvidence().stream()
                .map(Evidence::anomalyId).filter(Objects::nonNull)
                .map(byId::get).filter(Objects::nonNull).toList();
        String waitedStepTimeout = "workflow:" + wait.stepId() + ".timeout";
        return !cited.isEmpty() && cited.stream().allMatch(a ->
                a.getAnomalyType() == AnomalyType.STEP_TIMEOUT
                        && waitedStepTimeout.equals(a.getThresholdSource()));
    }

    private static boolean allStuckFindingsWithin(List<Anomaly> anomalies, Wait wait) {
        List<Anomaly> stuck = anomalies.stream()
                .filter(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK)
                .filter(a -> a.getDetectedAt() != null).toList();
        return !stuck.isEmpty() && stuck.stream().allMatch(a ->
                !a.getDetectedAt().isBefore(wait.from()) && !a.getDetectedAt().isAfter(wait.to()));
    }

    private static Instant heardAt(SagaEvent event) {
        return event.getReceivedAt() != null ? event.getReceivedAt() : event.getTimestamp();
    }
}
