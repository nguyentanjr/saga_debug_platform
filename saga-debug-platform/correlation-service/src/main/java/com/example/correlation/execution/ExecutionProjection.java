package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.workflow.WorkflowSpec;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public record ExecutionProjection(
        String sagaId,
        List<Operation> operations,
        List<SagaEvent> unmappedEvents) {
    public enum StartProvenance {
        OBSERVED_START,

        OBSERVED_CAUSAL_LINK,

        INFERRED_FROM_PREDECESSOR,

        INFERRED_FROM_JOIN,

        NO_START_REFERENCE
    }

    public enum OperationOutcome {
        SUCCEEDED,
        FAILED,

        IN_FLIGHT
    }

    public record Attempt(
            int ordinal,
            Instant startedAt,
            Instant endedAt,
            String startEventId,
            String endEventId) {
        public Duration duration() {
            return startedAt == null || endedAt == null ? null : Duration.between(startedAt, endedAt);
        }
    }

    public record Operation(
            String stepId,
            String serviceName,
            Instant startedAt,
            Instant endedAt,
            StartProvenance startProvenance,
            OperationOutcome outcome,
            List<Attempt> attempts,
            String startEventId,
            String endEventId,
            List<String> evidenceEventIds) {
        public Duration duration() {
            return startedAt == null || endedAt == null ? null : Duration.between(startedAt, endedAt);
        }

        public int attemptCount() {
            return attempts.size();
        }

        public boolean hasObservedStart() {
            return startProvenance == StartProvenance.OBSERVED_START;
        }

        public boolean hasMeasurableDuration() {
            return duration() != null;
        }
    }

    public Optional<Operation> operation(String stepId) {
        return operations.stream().filter(op -> op.stepId().equals(stepId)).findFirst();
    }

    public Map<String, Operation> byStep() {
        Map<String, Operation> map = new LinkedHashMap<>();
        for (Operation op : operations) {
            map.putIfAbsent(op.stepId(), op);
        }
        return map;
    }

    public List<Operation> measurableOperations() {
        return operations.stream().filter(Operation::hasMeasurableDuration).toList();
    }

    public static ExecutionProjection empty(String sagaId) {
        return new ExecutionProjection(sagaId, List.of(), List.of());
    }

    public static boolean isMapped(WorkflowSpec spec, String eventType) {
        return spec != null && spec.classifyEvent(eventType).isPresent();
    }
}
