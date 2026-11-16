package com.example.correlation.workflow;

import com.example.correlation.domain.SagaStatus;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowSpec {
    private String name;
    private String version;

    private List<String> sources = new ArrayList<>();

    private String runtimeScope;
    private List<StepSpec> steps;
    private List<StepSpec> compensationSteps;
    private List<TransitionSpec> transitions;
    private Map<String, String> validEventTypes;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<String> getSources() { return sources; }

    public String getRuntimeScope() { return runtimeScope; }
    public void setRuntimeScope(String runtimeScope) { this.runtimeScope = runtimeScope; }
    public void setSources(List<String> sources) {
        this.sources = sources != null ? sources : new ArrayList<>();
    }

    public boolean describesSource(String sourceName) {
        return sources.isEmpty() || (sourceName != null && sources.contains(sourceName));
    }

    public boolean isSourceScoped() {
        return !sources.isEmpty();
    }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public List<StepSpec> getSteps() { return steps; }
    public void setSteps(List<StepSpec> steps) { this.steps = steps; }

    public List<StepSpec> getCompensationSteps() { return compensationSteps; }
    public void setCompensationSteps(List<StepSpec> compensationSteps) { this.compensationSteps = compensationSteps; }

    public List<TransitionSpec> getTransitions() { return transitions; }
    public void setTransitions(List<TransitionSpec> transitions) { this.transitions = transitions; }

    public Map<String, String> getValidEventTypes() { return validEventTypes; }
    public void setValidEventTypes(Map<String, String> validEventTypes) { this.validEventTypes = validEventTypes; }

    public String getExpectedStepForEvent(String eventType) {
        String mapped = validEventTypes != null ? validEventTypes.get(eventType) : null;
        if (mapped != null) return mapped;
        return classifyEvent(eventType).map(EventRole::stepId).orElse(null);
    }

    public record EventRole(String stepId, Role role, String outcome) {
        public enum Role { STARTED, COMPLETED, FAILED, RETRY }

        public EventRole(String stepId, Role role) {
            this(stepId, role, defaultOutcome(role));
        }

        private static String defaultOutcome(Role role) {
            return switch (role) {
                case COMPLETED -> SUCCESS_OUTCOME;
                case FAILED -> FAILED_OUTCOME;
                case STARTED, RETRY -> null;
            };
        }
    }

    public static final String SUCCESS_OUTCOME = "SUCCESS";
    public static final String FAILED_OUTCOME = "FAILED";

    public Optional<EventRole> classifyEvent(String eventType) {
        if (eventType == null) return Optional.empty();

        EventRole best = null;
        for (StepSpec step : allSteps()) {
            if (step.getId() == null) continue;

            if (step.getEvents() != null) {
                for (Map.Entry<String, String> entry : step.getEvents().entrySet()) {
                    if (!eventType.equals(entry.getValue())) continue;
                    EventRole.Role role = toRole(entry.getKey());
                    if (role == null) continue;
                    best = better(best, new EventRole(step.getId(), role));
                }
            }

            if (step.getOutcomes() != null) {
                for (Map.Entry<String, String> entry : step.getOutcomes().entrySet()) {
                    if (!eventType.equals(entry.getValue())) continue;
                    String outcome = entry.getKey();
                    best = better(best, new EventRole(
                            step.getId(), roleOfOutcome(step.getId(), outcome), outcome));
                }
            }
        }
        return Optional.ofNullable(best);
    }

    private EventRole.Role roleOfOutcome(String stepId, String outcome) {
        if (FAILED_OUTCOME.equals(outcome)) return EventRole.Role.FAILED;
        boolean routesToFailure = outgoingTargets(stepId, outcome).stream()
                .anyMatch(target -> TERMINAL_TARGETS.get(target) == SagaStatus.FAILED);
        return routesToFailure ? EventRole.Role.FAILED : EventRole.Role.COMPLETED;
    }

    private static EventRole better(EventRole current, EventRole candidate) {
        if (current == null) return candidate;
        return rolePrecedence(candidate.role()) < rolePrecedence(current.role())
                ? candidate : current;
    }

    public Optional<String> predecessorStep(String stepId) {
        if (transitions == null || stepId == null) return Optional.empty();

        List<String> candidates = predecessorSteps(stepId);
        return candidates.size() == 1 ? Optional.of(candidates.get(0)) : Optional.empty();
    }

    public List<String> predecessorSteps(String stepId) {
        if (transitions == null || stepId == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId.equals(t.getTo()))
                .map(TransitionSpec::getFrom)
                .filter(from -> from != null && hasStep(from))
                .distinct()
                .toList();
    }

    public boolean isJoin(String stepId) {
        List<String> predecessors = predecessorSteps(stepId);
        if (predecessors.size() < 2) return false;

        for (String first : predecessors) {
            for (String second : predecessors) {
                if (first.equals(second)) continue;
                if (isUpstreamOf(first, second)) return false;
            }
        }
        return true;
    }

    public boolean areConcurrent(String first, String second) {
        if (first == null || second == null || first.equals(second)) return false;
        if (!hasStep(first) || !hasStep(second)) return false;

        if (isUpstreamOf(first, second) || isUpstreamOf(second, first)) return false;

        return onSeparateArmsOfOneFanOut(first, second);
    }

    private boolean onSeparateArmsOfOneFanOut(String first, String second) {
        if (transitions == null) return false;

        Map<List<String>, Set<String>> armsByFanOut = new LinkedHashMap<>();
        for (TransitionSpec transition : transitions) {
            if (transition.getFrom() == null || transition.getTo() == null) continue;
            if (!hasStep(transition.getTo())) continue;
            armsByFanOut
                    .computeIfAbsent(
                            List.of(transition.getFrom(), String.valueOf(transition.getCondition())),
                            key -> new LinkedHashSet<>())
                    .add(transition.getTo());
        }

        for (Set<String> arms : armsByFanOut.values()) {
            if (arms.size() < 2) continue;
            for (String armOfFirst : arms) {
                if (!leadsTo(armOfFirst, first)) continue;
                for (String armOfSecond : arms) {
                    if (armOfSecond.equals(armOfFirst)) continue;
                    if (leadsTo(armOfSecond, second)) return true;
                }
            }
        }
        return false;
    }

    private boolean leadsTo(String arm, String step) {
        return arm.equals(step) || isUpstreamOf(arm, step);
    }

    public int upstreamStepCount(String stepId) {
        if (stepId == null) return 0;
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>(predecessorSteps(stepId));
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            if (current == null || current.equals(stepId) || !visited.add(current)) continue;
            frontier.addAll(predecessorSteps(current));
        }
        return visited.size();
    }

    public boolean hasStep(String stepId) {
        return allSteps().stream().anyMatch(s -> s.getId() != null && s.getId().equals(stepId));
    }

    public Optional<String> serviceOf(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .map(StepSpec::getService)
                .filter(service -> service != null && !service.isBlank())
                .findFirst();
    }

    public List<String> stepsAfter(String stepId) {
        if (stepId == null) return List.of();
        return allSteps().stream()
                .map(StepSpec::getId)
                .filter(Objects::nonNull)
                .filter(candidate -> predecessorSteps(candidate).contains(stepId))
                .distinct()
                .toList();
    }

    private List<StepSpec> allSteps() {
        List<StepSpec> all = new ArrayList<>();
        if (steps != null) all.addAll(steps);
        if (compensationSteps != null) all.addAll(compensationSteps);
        return all;
    }

    private static EventRole.Role toRole(String eventsKey) {
        if (isRetryKey(eventsKey)) return EventRole.Role.RETRY;
        return switch (eventsKey) {
            case "started" -> EventRole.Role.STARTED;
            case "completed" -> EventRole.Role.COMPLETED;
            case "failed" -> EventRole.Role.FAILED;
            default -> null;
        };
    }

    private static boolean isRetryKey(String eventsKey) {
        return eventsKey.startsWith("retry");
    }

    public boolean isRepeatWithoutProgress(String eventType) {
        return classifyEvent(eventType)
                .filter(role -> role.role() == EventRole.Role.RETRY)
                .map(role -> allSteps().stream()
                        .anyMatch(step -> role.stepId().equals(step.getId()) && step.isRetriesAreNotProgress()))
                .orElse(false);
    }

    private static int rolePrecedence(EventRole.Role role) {
        return switch (role) {
            case COMPLETED -> 0;
            case FAILED -> 1;
            case RETRY -> 2;
            case STARTED -> 3;
        };
    }

    public enum TransitionVerdict { VALID, INVALID, UNKNOWN }

    public record TransitionJudgment(TransitionVerdict verdict, List<String> stepsInBetween) {
        static TransitionJudgment of(TransitionVerdict verdict) {
            return new TransitionJudgment(verdict, List.of());
        }
    }

    public TransitionVerdict judgeTransition(String fromStep, String toStep, String condition) {
        return explainTransition(fromStep, toStep, condition).verdict();
    }

    public TransitionJudgment explainTransition(String fromStep, String toStep, String condition) {
        if (transitions == null || transitions.isEmpty()) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        if (fromStep != null && fromStep.equals(toStep)) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        boolean anyFromThisStep = transitions.stream()
                .anyMatch(t -> t.getFrom() != null && t.getFrom().equals(fromStep));
        if (!anyFromThisStep) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        boolean exactMatch = transitions.stream().anyMatch(t ->
                t.getFrom().equals(fromStep) &&
                t.getTo().equals(toStep) &&
                (condition == null || condition.equals(t.getCondition())));
        if (exactMatch) return TransitionJudgment.of(TransitionVerdict.VALID);

        if (areConcurrent(fromStep, toStep)) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        Set<String> declared = new LinkedHashSet<>(outgoingTargets(fromStep, condition));
        declared.addAll(unconditionedTargets(fromStep));
        if (declared.isEmpty()) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        Set<String> successors = stepSuccessors(declared);
        if (successors.contains(toStep)) {
            return TransitionJudgment.of(TransitionVerdict.VALID);
        }
        if (successors.isEmpty()) {
            return TransitionJudgment.of(TransitionVerdict.INVALID);
        }

        Set<String> downstream = forwardClosure(successors);
        if (!downstream.contains(toStep)) {
            return TransitionJudgment.of(TransitionVerdict.INVALID);
        }

        List<String> inBetween = downstream.stream()
                .filter(step -> !step.equals(toStep))
                .filter(step -> forwardClosure(Set.of(step)).contains(toStep))
                .toList();
        return new TransitionJudgment(TransitionVerdict.UNKNOWN, inBetween);
    }

    private List<String> unconditionedTargets(String stepId) {
        if (transitions == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId != null && stepId.equals(t.getFrom()))
                .filter(t -> t.getCondition() == null)
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }

    private Set<String> stepSuccessors(Set<String> declaredTargets) {
        Set<String> successors = new LinkedHashSet<>();
        for (String target : declaredTargets) {
            if (hasStep(target)) {
                successors.add(target);
            } else if (COMPENSATING_MARKER.equals(target) && compensationSteps != null) {
                compensationSteps.stream()
                        .map(StepSpec::getId)
                        .filter(Objects::nonNull)
                        .forEach(successors::add);
            }
        }
        return successors;
    }

    private Set<String> forwardClosure(Set<String> seeds) {
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>(seeds);
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            if (current == null || !visited.add(current)) continue;
            if (transitions == null) continue;
            Set<String> targets = transitions.stream()
                    .filter(t -> current.equals(t.getFrom()))
                    .map(TransitionSpec::getTo)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            frontier.addAll(stepSuccessors(targets));
        }
        return visited;
    }

    public boolean isValidTransition(String fromStep, String toStep, String condition) {
        return judgeTransition(fromStep, toStep, condition) != TransitionVerdict.INVALID;
    }

    public boolean isUpstreamOf(String candidate, String step) {
        if (candidate == null || step == null || candidate.equals(step)) return false;

        Set<String> visited = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>(predecessorSteps(step));
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            if (current == null || !visited.add(current)) continue;
            if (candidate.equals(current)) return true;
            frontier.addAll(predecessorSteps(current));
        }
        return false;
    }

    public int getStepTimeoutSeconds(String stepId) {
        if (steps != null) {
            for (StepSpec step : steps) {
                if (step.getId().equals(stepId)) {
                    return parseTimeout(step.getTimeout());
                }
            }
        }
        if (compensationSteps != null) {
            for (StepSpec step : compensationSteps) {
                if (step.getId().equals(stepId)) {
                    return parseTimeout(step.getTimeout());
                }
            }
        }
        return 30;
    }

    private int parseTimeout(String timeout) {
        if (timeout == null) return 30;
        timeout = timeout.trim().toLowerCase();
        if (timeout.endsWith("s")) {
            return Integer.parseInt(timeout.replace("s", ""));
        } else if (timeout.endsWith("m")) {
            return Integer.parseInt(timeout.replace("m", "")) * 60;
        }
        return 30;
    }

    public Set<String> retryEventTypes() {
        return allSteps().stream()
                .map(StepSpec::getEvents)
                .filter(Objects::nonNull)
                .flatMap(events -> events.entrySet().stream())
                .filter(entry -> isRetryKey(entry.getKey()))
                .map(Map.Entry::getValue)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public Optional<Integer> declaredMaxAttempts(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .map(StepSpec::getRetryPolicy)
                .filter(Objects::nonNull)
                .map(RetryPolicySpec::getMaxAttempts)
                .filter(Objects::nonNull)
                .findFirst();
    }

    private static final Map<String, SagaStatus> TERMINAL_TARGETS = Map.of(
            "COMPLETED", SagaStatus.COMPLETED,
            "FAILED", SagaStatus.FAILED,
            "COMPENSATING", SagaStatus.COMPENSATING,
            "COMPENSATED", SagaStatus.COMPENSATED);

    private static final String START_MARKER = "STARTED";

    private static final String COMPENSATING_MARKER = "COMPENSATING";

    public Set<String> startEventTypes() {
        if (transitions == null) return Set.of();
        return transitions.stream()
                .filter(t -> START_MARKER.equals(t.getFrom()))
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .flatMap(stepId -> lifecycleEvents(stepId).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public Set<String> declaredEventTypes() {
        return allSteps().stream()
                .flatMap(WorkflowSpec::allDeclaredEvents)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Stream<String> allDeclaredEvents(StepSpec step) {
        return Stream.concat(
                        step.getEvents() != null ? step.getEvents().values().stream() : Stream.empty(),
                        step.getOutcomes() != null ? step.getOutcomes().values().stream() : Stream.empty())
                .filter(Objects::nonNull);
    }

    public boolean declaresCompletionEvent(String stepId) {
        if (stepId == null) return false;
        for (StepSpec step : allSteps()) {
            if (step.getId() == null || !step.getId().equals(stepId)) continue;

            if (step.getEvents() != null && step.getEvents().keySet().stream()
                    .map(WorkflowSpec::toRole)
                    .anyMatch(role -> role == EventRole.Role.COMPLETED)) {
                return true;
            }
            if (step.getOutcomes() != null && step.getOutcomes().keySet().stream()
                    .anyMatch(outcome -> roleOfOutcome(stepId, outcome) == EventRole.Role.COMPLETED)) {
                return true;
            }
        }
        return false;
    }

    public Optional<String> compensationOf(String stepId) {
        if (steps == null || stepId == null) return Optional.empty();
        return steps.stream().filter(s -> stepId.equals(s.getId()))
                .map(StepSpec::getCompensation).filter(Objects::nonNull).findFirst();
    }

    public boolean isCompensationStep(String stepId) {
        return compensationSteps != null && compensationSteps.stream()
                .anyMatch(s -> s.getId() != null && s.getId().equals(stepId));
    }

    public Optional<SagaStatus> statusAfter(EventRole role) {
        if (role == null) return Optional.empty();

        if (isCompensationStep(role.stepId())) {
            return switch (role.role()) {
                case STARTED -> Optional.of(SagaStatus.COMPENSATING);
                case COMPLETED -> Optional.of(SagaStatus.COMPENSATED);
                case FAILED -> Optional.of(SagaStatus.COMPENSATING);
                case RETRY -> Optional.empty();
            };
        }

        if (role.role() == EventRole.Role.STARTED) return Optional.of(SagaStatus.IN_PROGRESS);
        if (role.role() == EventRole.Role.RETRY) return Optional.empty();

        String condition = role.outcome() != null ? role.outcome() : SUCCESS_OUTCOME;

        Set<SagaStatus> implied = outgoingTargets(role.stepId(), condition).stream()
                .map(target -> TERMINAL_TARGETS.getOrDefault(target, SagaStatus.IN_PROGRESS))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return implied.size() == 1 ? Optional.of(implied.iterator().next()) : Optional.empty();
    }

    public List<String> stepsAwaitedAfter(String eventType) {
        Optional<EventRole> role = classifyEvent(eventType);
        if (role.isEmpty()) return List.of();
        EventRole r = role.get();
        if (r.role() == EventRole.Role.STARTED || r.role() == EventRole.Role.RETRY) {
            return List.of(r.stepId());
        }
        String condition = r.outcome() != null ? r.outcome()
                : r.role() == EventRole.Role.FAILED ? "FAILED" : SUCCESS_OUTCOME;
        Set<String> stepIds = allSteps().stream().map(StepSpec::getId).collect(Collectors.toSet());
        return outgoingTargets(r.stepId(), condition).stream().filter(stepIds::contains).toList();
    }

    private List<String> outgoingTargets(String stepId, String condition) {
        if (transitions == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId != null && stepId.equals(t.getFrom()))
                .filter(t -> condition.equals(t.getCondition()))
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }

    private Set<String> lifecycleEvents(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .flatMap(WorkflowSpec::allDeclaredEvents)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RetryPolicySpec {
        private Integer maxAttempts;
        private String backoff;

        public Integer getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }

        public String getBackoff() { return backoff; }
        public void setBackoff(String backoff) { this.backoff = backoff; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StepSpec {
        private String id;
        private String service;
        private String timeout;
        private String compensation;
        private RetryPolicySpec retryPolicy;
        private Map<String, String> events;

        private Map<String, String> outcomes;

        private boolean retriesAreNotProgress;

        public boolean isRetriesAreNotProgress() { return retriesAreNotProgress; }
        public void setRetriesAreNotProgress(boolean retriesAreNotProgress) { this.retriesAreNotProgress = retriesAreNotProgress; }

        public Map<String, String> getOutcomes() { return outcomes; }
        public void setOutcomes(Map<String, String> outcomes) { this.outcomes = outcomes; }

        public RetryPolicySpec getRetryPolicy() { return retryPolicy; }
        public void setRetryPolicy(RetryPolicySpec retryPolicy) { this.retryPolicy = retryPolicy; }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public String getService() { return service; }
        public void setService(String service) { this.service = service; }

        public String getTimeout() { return timeout; }
        public void setTimeout(String timeout) { this.timeout = timeout; }

        public String getCompensation() { return compensation; }
        public void setCompensation(String compensation) { this.compensation = compensation; }

        public Map<String, String> getEvents() { return events; }
        public void setEvents(Map<String, String> events) { this.events = events; }
    }

    public static class TransitionSpec {
        private String from;
        private String to;
        private String condition;

        public String getFrom() { return from; }
        public void setFrom(String from) { this.from = from; }

        public String getTo() { return to; }
        public void setTo(String to) { this.to = to; }

        public String getCondition() { return condition; }
        public void setCondition(String condition) { this.condition = condition; }
    }
}
