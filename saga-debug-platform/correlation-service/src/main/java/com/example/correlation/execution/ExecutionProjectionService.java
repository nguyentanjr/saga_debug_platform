package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjection.Attempt;
import com.example.correlation.execution.ExecutionProjection.Operation;
import com.example.correlation.execution.ExecutionProjection.OperationOutcome;
import com.example.correlation.execution.ExecutionProjection.StartProvenance;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpec.EventRole;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class ExecutionProjectionService {
    private static final StartRef NO_START = new StartRef(null, null, StartProvenance.NO_START_REFERENCE);

    public ExecutionProjection project(String sagaId, List<SagaEvent> events, WorkflowSpec spec) {
        if (events == null || events.isEmpty()) {
            return ExecutionProjection.empty(sagaId);
        }
        if (spec == null) {
            return new ExecutionProjection(sagaId, List.of(), List.copyOf(events));
        }

        List<SagaEvent> ordered = events.stream()
                .filter(e -> e.getTimestamp() != null)
                .sorted(Comparator.comparing(SagaEvent::getTimestamp)
                        .thenComparing(SagaEvent::getEventId, Comparator.nullsLast(String::compareTo)))
                .toList();

        Map<String, SagaEvent> byEventId = new LinkedHashMap<>();
        Map<String, SagaEvent> bySpanId = new LinkedHashMap<>();
        for (SagaEvent event : ordered) {
            if (event.getEventId() != null) byEventId.putIfAbsent(event.getEventId(), event);
            if (event.getSpanId() != null) bySpanId.putIfAbsent(event.getSpanId(), event);
        }

        Map<String, List<RoleObservation>> byStep = new LinkedHashMap<>();
        List<SagaEvent> unmapped = new ArrayList<>();

        for (SagaEvent event : ordered) {
            Optional<EventRole> role = spec.classifyEvent(event.getEventType());
            if (role.isEmpty()) {
                unmapped.add(event);
                continue;
            }
            byStep.computeIfAbsent(role.get().stepId(), k -> new ArrayList<>())
                    .add(new RoleObservation(event, role.get().role()));
        }

        Map<String, SagaEvent> terminalByStep = new LinkedHashMap<>();
        byStep.forEach((stepId, observations) ->
                terminalOf(observations).ifPresent(obs -> terminalByStep.put(stepId, obs.event())));

        CausalIndex causal = new CausalIndex(byEventId, bySpanId);

        List<Operation> operations = new ArrayList<>();
        for (Map.Entry<String, List<RoleObservation>> entry : byStep.entrySet()) {
            operations.add(buildOperation(entry.getKey(), entry.getValue(), spec, terminalByStep, causal));
        }
        operations.sort(Comparator.comparing(
                op -> op.startedAt() != null ? op.startedAt() : op.endedAt(),
                Comparator.nullsLast(Comparator.naturalOrder())));

        return new ExecutionProjection(sagaId, List.copyOf(operations), List.copyOf(unmapped));
    }

    private Operation buildOperation(String stepId,
            List<RoleObservation> observations,
            WorkflowSpec spec,
            Map<String, SagaEvent> terminalByStep,
            CausalIndex causal) {
        Optional<RoleObservation> terminal = terminalOf(observations);
        List<RoleObservation> retries = observations.stream()
                .filter(o -> o.role() == EventRole.Role.RETRY)
                .toList();

        StartRef startRef = resolveStart(stepId, observations, terminal, spec, terminalByStep, causal);

        SagaEvent endEvent = terminal.map(RoleObservation::event).orElse(null);
        OperationOutcome outcome = terminal
                .map(o -> o.role() == EventRole.Role.FAILED
                        ? OperationOutcome.FAILED
                        : OperationOutcome.SUCCEEDED)
                .orElse(OperationOutcome.IN_FLIGHT);

        List<Attempt> attempts = buildAttempts(startRef, retries, endEvent);

        List<String> evidence = observations.stream()
                .map(o -> o.event().getEventId())
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(ArrayList::new));
        if (startRef.event() != null && startRef.event().getEventId() != null
                && !evidence.contains(startRef.event().getEventId())) {
            evidence.add(0, startRef.event().getEventId());
        }

        return new Operation(
                stepId,
                serviceOf(observations, spec, stepId),
                startRef.instant(),
                endEvent != null ? endEvent.getTimestamp() : null,
                startRef.provenance(),
                outcome,
                attempts,
                startRef.event() != null ? startRef.event().getEventId() : null,
                endEvent != null ? endEvent.getEventId() : null,
                List.copyOf(evidence));
    }

    private StartRef resolveStart(String stepId,
            List<RoleObservation> observations,
            Optional<RoleObservation> terminal,
            WorkflowSpec spec,
            Map<String, SagaEvent> terminalByStep,
            CausalIndex causal) {
        Optional<RoleObservation> observedStart = observations.stream()
                .filter(o -> o.role() == EventRole.Role.STARTED)
                .findFirst();

        boolean startIsDistinct = observedStart.isPresent()
                && (terminal.isEmpty() || observedStart.get().event() != terminal.get().event());

        if (startIsDistinct) {
            SagaEvent start = observedStart.get().event();
            return new StartRef(start, start.getTimestamp(), StartProvenance.OBSERVED_START);
        }

        SagaEvent cause = observations.stream()
                .map(RoleObservation::event)
                .findFirst()
                .flatMap(causal::causeOf)
                .orElse(null);
        if (cause != null && cause.getTimestamp() != null) {
            return new StartRef(cause, cause.getTimestamp(), StartProvenance.OBSERVED_CAUSAL_LINK);
        }

        List<String> predecessors = spec.predecessorSteps(stepId);
        if (predecessors.isEmpty()) {
            return NO_START;
        }

        List<SagaEvent> predecessorTerminals = predecessors.stream()
                .map(terminalByStep::get)
                .filter(Objects::nonNull)
                .toList();

        boolean isJoin = spec.isJoin(stepId);
        boolean joinStillWaiting = isJoin && predecessorTerminals.size() != predecessors.size();
        if (joinStillWaiting || predecessorTerminals.isEmpty()) {
            return NO_START;
        }

        SagaEvent latest = predecessorTerminals.stream()
                .max(Comparator.comparing(SagaEvent::getTimestamp))
                .orElse(null);
        if (latest == null) {
            return NO_START;
        }

        return new StartRef(latest, latest.getTimestamp(),
                isJoin ? StartProvenance.INFERRED_FROM_JOIN
                        : StartProvenance.INFERRED_FROM_PREDECESSOR);
    }

    private List<Attempt> buildAttempts(StartRef startRef, List<RoleObservation> retries, SagaEvent endEvent) {
        List<Attempt> attempts = new ArrayList<>();
        Instant cursorInstant = startRef.instant();
        String cursorEventId = startRef.event() != null ? startRef.event().getEventId() : null;

        int ordinal = 1;
        for (RoleObservation retry : retries) {
            attempts.add(new Attempt(ordinal++, cursorInstant, retry.event().getTimestamp(),
                    cursorEventId, retry.event().getEventId()));
            cursorInstant = retry.event().getTimestamp();
            cursorEventId = retry.event().getEventId();
        }

        if (endEvent != null) {
            attempts.add(new Attempt(ordinal, cursorInstant, endEvent.getTimestamp(),
                    cursorEventId, endEvent.getEventId()));
        } else if (attempts.isEmpty()) {
            attempts.add(new Attempt(ordinal, cursorInstant, null, cursorEventId, null));
        }

        return List.copyOf(attempts);
    }

    private static Optional<RoleObservation> terminalOf(List<RoleObservation> observations) {
        return observations.stream()
                .filter(o -> o.role() == EventRole.Role.COMPLETED || o.role() == EventRole.Role.FAILED)
                .reduce((first, second) -> second);
    }

    private static String serviceOf(List<RoleObservation> observations, WorkflowSpec spec, String stepId) {
        return observations.stream()
                .map(o -> o.event().getServiceName())
                .filter(Objects::nonNull)
                .findFirst()
                .orElseGet(() -> declaredService(spec, stepId));
    }

    private static String declaredService(WorkflowSpec spec, String stepId) {
        List<WorkflowSpec.StepSpec> all = new ArrayList<>();
        if (spec.getSteps() != null) all.addAll(spec.getSteps());
        if (spec.getCompensationSteps() != null) all.addAll(spec.getCompensationSteps());
        return all.stream()
                .filter(s -> stepId.equals(s.getId()))
                .map(WorkflowSpec.StepSpec::getService)
                .findFirst()
                .orElse(null);
    }

    private record RoleObservation(SagaEvent event, EventRole.Role role) {
    }

    private record CausalIndex(Map<String, SagaEvent> byEventId, Map<String, SagaEvent> bySpanId) {
        Optional<SagaEvent> causeOf(SagaEvent event) {
            if (event == null) return Optional.empty();

            SagaEvent viaCausation = event.getCausationId() != null
                    ? byEventId.get(event.getCausationId())
                    : null;
            if (viaCausation != null && viaCausation != event) {
                return Optional.of(viaCausation);
            }

            SagaEvent viaSpan = event.getParentSpanId() != null
                    ? bySpanId.get(event.getParentSpanId())
                    : null;
            return viaSpan != null && viaSpan != event ? Optional.of(viaSpan) : Optional.empty();
        }
    }

    private record StartRef(SagaEvent event, Instant instant, StartProvenance provenance) {
    }
}
