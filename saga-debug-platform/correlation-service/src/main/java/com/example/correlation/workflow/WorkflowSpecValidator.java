package com.example.correlation.workflow;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class WorkflowSpecValidator {
    private static final Set<String> TERMINAL_MARKERS =
            Set.of("COMPLETED", "FAILED", "COMPENSATING", "COMPENSATED");

    private static final String START_MARKER = "STARTED";

    private WorkflowSpecValidator() {}

    public record Report(String workflowName, List<String> errors, List<String> warnings) {
        public boolean isUsable() { return errors.isEmpty(); }

        public boolean isClean() { return errors.isEmpty() && warnings.isEmpty(); }
    }

    public static Report validate(WorkflowSpec spec) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        if (spec == null) {
            return new Report(null, List.of("spec is null"), List.of());
        }

        String name = spec.getName();
        if (name == null || name.isBlank()) {
            errors.add("workflow has no name, so no execution can be matched to it");
        }

        List<WorkflowSpec.StepSpec> steps = nonNull(spec.getSteps());
        List<WorkflowSpec.StepSpec> compensations = nonNull(spec.getCompensationSteps());
        List<WorkflowSpec.StepSpec> allSteps = new ArrayList<>(steps);
        allSteps.addAll(compensations);

        if (allSteps.isEmpty()) {
            errors.add("workflow declares no steps");
            return new Report(name, errors, warnings);
        }

        Set<String> stepIds = new LinkedHashSet<>();
        for (WorkflowSpec.StepSpec step : allSteps) {
            if (step.getId() == null || step.getId().isBlank()) {
                errors.add("a step has no id");
            } else if (!stepIds.add(step.getId())) {
                errors.add("duplicate step id '" + step.getId() + "'");
            }
        }

        List<WorkflowSpec.TransitionSpec> transitions = nonNull(spec.getTransitions());
        if (transitions.isEmpty()) {
            warnings.add("no transitions declared, so no execution can reach a terminal "
                    + "state and no transition can be judged invalid");
            checkEventDeclarations(spec, allSteps, stepIds, errors, warnings);
            return new Report(name, errors, warnings);
        }

        checkTransitionEndpoints(transitions, stepIds, errors);
        checkStart(transitions, stepIds, errors);
        checkReachability(transitions, stepIds, steps, errors);
        checkOutgoing(transitions, steps, warnings);
        checkTerminalReachable(transitions, warnings);
        checkCompensationTargets(steps, stepIds, errors);
        checkEventDeclarations(spec, allSteps, stepIds, errors, warnings);
        checkOutcomeRouting(allSteps, transitions, warnings);

        return new Report(name, errors, warnings);
    }

    private static void checkTransitionEndpoints(List<WorkflowSpec.TransitionSpec> transitions,
            Set<String> stepIds, List<String> errors) {
        for (WorkflowSpec.TransitionSpec t : transitions) {
            if (t.getFrom() == null || t.getTo() == null) {
                errors.add("a transition is missing 'from' or 'to'");
                continue;
            }
            if (!known(t.getFrom(), stepIds)) {
                errors.add("transition from unknown step '" + t.getFrom() + "'");
            }
            if (!known(t.getTo(), stepIds)) {
                errors.add("transition to unknown step '" + t.getTo() + "'");
            }
        }
    }

    private static void checkStart(List<WorkflowSpec.TransitionSpec> transitions,
            Set<String> stepIds, List<String> errors) {
        List<String> firstSteps = transitions.stream()
                .filter(t -> START_MARKER.equals(t.getFrom()))
                .map(WorkflowSpec.TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .filter(stepIds::contains)
                .toList();

        if (firstSteps.isEmpty()) {
            errors.add("no 'STARTED -> <step>' transition, so no event can be recognised "
                    + "as starting this workflow and every execution looks orphaned");
        }
    }

    private static void checkReachability(List<WorkflowSpec.TransitionSpec> transitions,
            Set<String> stepIds, List<WorkflowSpec.StepSpec> forwardSteps, List<String> errors) {
        Set<String> reached = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>();
        frontier.add(START_MARKER);

        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            for (WorkflowSpec.TransitionSpec t : transitions) {
                if (current.equals(t.getFrom()) && t.getTo() != null && reached.add(t.getTo())) {
                    frontier.add(t.getTo());
                }
            }
        }

        for (WorkflowSpec.StepSpec step : forwardSteps) {
            if (step.getId() != null && stepIds.contains(step.getId())
                    && !reached.contains(step.getId())) {
                errors.add("step '" + step.getId() + "' is unreachable from STARTED, so it "
                        + "can never be entered and its timeout can never fire");
            }
        }
    }

    private static void checkOutgoing(List<WorkflowSpec.TransitionSpec> transitions,
            List<WorkflowSpec.StepSpec> forwardSteps, List<String> warnings) {
        for (WorkflowSpec.StepSpec step : forwardSteps) {
            if (step.getId() == null) continue;
            boolean hasOutgoing = transitions.stream()
                    .anyMatch(t -> step.getId().equals(t.getFrom()));
            if (!hasOutgoing) {
                warnings.add("step '" + step.getId() + "' has no outgoing transition, so an "
                        + "execution reaching it never resolves to a terminal state");
            }
        }
    }

    private static void checkTerminalReachable(List<WorkflowSpec.TransitionSpec> transitions,
            List<String> warnings) {
        boolean reachesTerminal = transitions.stream()
                .map(WorkflowSpec.TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .anyMatch(TERMINAL_MARKERS::contains);
        if (!reachesTerminal) {
            warnings.add("no transition leads to COMPLETED, FAILED, COMPENSATING or "
                    + "COMPENSATED, so no execution of this workflow can ever be judged finished");
        }
    }

    private static void checkCompensationTargets(List<WorkflowSpec.StepSpec> forwardSteps,
            Set<String> stepIds, List<String> errors) {
        for (WorkflowSpec.StepSpec step : forwardSteps) {
            String compensation = step.getCompensation();
            if (compensation != null && !compensation.isBlank() && !stepIds.contains(compensation)) {
                errors.add("step '" + step.getId() + "' declares compensation '" + compensation
                        + "', which is not a declared step");
            }
        }
    }

    private static void checkEventDeclarations(WorkflowSpec spec,
            List<WorkflowSpec.StepSpec> allSteps, Set<String> stepIds,
            List<String> errors, List<String> warnings) {
        Set<String> declaredBySteps = spec.declaredEventTypes();

        boolean anyStepDeclaresEvents = allSteps.stream()
                .anyMatch(s -> (s.getEvents() != null && !s.getEvents().isEmpty())
                        || (s.getOutcomes() != null && !s.getOutcomes().isEmpty()));
        if (!anyStepDeclaresEvents) {
            errors.add("no step declares any event type, so no observation can ever be "
                    + "attributed to a step");
        }

        var validEventTypes = spec.getValidEventTypes();
        if (validEventTypes == null) return;

        validEventTypes.forEach((eventType, stepId) -> {
            if (stepId != null && !stepIds.contains(stepId)) {
                errors.add("validEventTypes maps '" + eventType + "' to unknown step '"
                        + stepId + "'");
            }
            if (!declaredBySteps.contains(eventType)) {
                warnings.add("validEventTypes lists '" + eventType + "' but no step declares "
                        + "it, so classifyEvent() will not recognise it");
            }

            spec.classifyEvent(eventType)
                    .map(WorkflowSpec.EventRole::stepId)
                    .filter(declaredStep -> stepId != null && !declaredStep.equals(stepId))
                    .ifPresent(declaredStep -> warnings.add("validEventTypes places '" + eventType
                            + "' in step '" + stepId + "', but step '" + declaredStep
                            + "' declares it; the map wins, so check which one is right"));
        });
    }

    private static void checkOutcomeRouting(List<WorkflowSpec.StepSpec> allSteps,
            List<WorkflowSpec.TransitionSpec> transitions, List<String> warnings) {
        for (WorkflowSpec.StepSpec step : allSteps) {
            if (step.getId() == null || step.getOutcomes() == null) continue;

            for (String outcome : step.getOutcomes().keySet()) {
                boolean routed = transitions.stream()
                        .anyMatch(t -> step.getId().equals(t.getFrom())
                                && outcome.equals(t.getCondition()));
                if (!routed) {
                    warnings.add("step '" + step.getId() + "' declares outcome '" + outcome
                            + "' but no transition is conditioned on it, so that outcome "
                            + "leaves the saga's status undetermined");
                }
            }
        }
    }

    private static boolean known(String endpoint, Set<String> stepIds) {
        return stepIds.contains(endpoint)
                || TERMINAL_MARKERS.contains(endpoint)
                || START_MARKER.equals(endpoint);
    }

    private static <T> List<T> nonNull(List<T> list) {
        return list != null ? list : List.of();
    }
}
