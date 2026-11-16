package com.example.correlation.domain;

import java.time.Instant;

public record IngestedEvent(
        Identity identity,

        String declaredWorkflow,
        String eventType,
        String stepName,
        String serviceName,
        Instant eventTime,
        String payload,
        Instant receivedAt,
        Causality causality,

        String transportRef,

        EventTimeSource eventTimeSource) {
    public IngestedEvent(Identity identity, String declaredWorkflow, String eventType,
            String stepName, String serviceName, Instant eventTime, String payload,
            Instant receivedAt, Causality causality, String transportRef) {
        this(identity, declaredWorkflow, eventType, stepName, serviceName, eventTime,
                payload, receivedAt, causality, transportRef, null);
    }

    public IngestedEvent(Identity identity, String declaredWorkflow, String eventType,
            String stepName, String serviceName, Instant eventTime, String payload,
            Instant receivedAt, Causality causality) {
        this(identity, declaredWorkflow, eventType, stepName, serviceName, eventTime,
                payload, receivedAt, causality, null);
    }

    public record Identity(
            String sourceName,
            ExecutionKey executionKey,
            String eventId,
            String businessKey,
            CorrelationBasis correlationBasis) {
        public Identity {
            if (executionKey != null && sourceName != null
                    && !sourceName.equals(executionKey.source())) {
                throw new IllegalArgumentException(
                        "identity disagrees with itself: source '" + sourceName
                                + "' but key '" + executionKey.canonical() + "'");
            }
        }
    }

    public record Causality(
            String causationId,
            String spanId,
            String parentSpanId,
            String traceId) {
        public static final Causality NONE = new Causality(null, null, null, null);
    }

    public String sagaId() {
        return identity.executionKey() != null ? identity.executionKey().canonical() : null;
    }

    public String sourceName() { return identity.sourceName(); }
    public ExecutionKey executionKey() { return identity.executionKey(); }
    public String eventId() { return identity.eventId(); }
    public String businessKey() { return identity.businessKey(); }
    public CorrelationBasis correlationBasis() { return identity.correlationBasis(); }

    public String causationId() { return causality.causationId(); }
    public String spanId() { return causality.spanId(); }
    public String parentSpanId() { return causality.parentSpanId(); }
    public String traceId() { return causality.traceId(); }

    public boolean hasCausalLink() {
        return causality.causationId() != null || causality.parentSpanId() != null;
    }

    public static IngestedEvent withoutCausality(String eventId, String sagaId, String eventType,
            String stepName, String serviceName, Instant eventTime, String payload,
            Instant receivedAt) {
        return new IngestedEvent(
                new Identity(ExecutionKey.DEFAULT_SOURCE,
                        ExecutionKey.of(ExecutionKey.DEFAULT_SOURCE, sagaId),
                        eventId, null, CorrelationBasis.DECLARED_KEY),
                null, eventType, stepName, serviceName, eventTime, payload, receivedAt,
                Causality.NONE);
    }
}
