package com.example.correlation.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "saga_events")
public class SagaEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "saga_id", nullable = false)
    private String sagaId;

    @Column(name = "source_name", nullable = false)
    private String sourceName;

    @Enumerated(EnumType.STRING)
    @Column(name = "correlation_basis")
    private CorrelationBasis correlationBasis;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "step_name")
    private String stepName;

    @Column(name = "service_name")
    private String serviceName;

    @Column(nullable = false)
    private Instant timestamp;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_time_source", length = 20)
    private EventTimeSource eventTimeSource;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt = Instant.now();

    @Column(name = "causation_id")
    private String causationId;

    @Column(name = "span_id")
    private String spanId;

    @Column(name = "parent_span_id")
    private String parentSpanId;

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "transport_ref")
    private String transportRef;

    public String getCausationId() { return causationId; }
    public void setCausationId(String causationId) { this.causationId = causationId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getParentSpanId() { return parentSpanId; }
    public void setParentSpanId(String parentSpanId) { this.parentSpanId = parentSpanId; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getTransportRef() { return transportRef; }
    public void setTransportRef(String transportRef) { this.transportRef = transportRef; }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public CorrelationBasis getCorrelationBasis() { return correlationBasis; }
    public void setCorrelationBasis(CorrelationBasis basis) { this.correlationBasis = basis; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getStepName() { return stepName; }
    public void setStepName(String stepName) { this.stepName = stepName; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public EventTimeSource getEventTimeSource() { return eventTimeSource; }
    public void setEventTimeSource(EventTimeSource eventTimeSource) { this.eventTimeSource = eventTimeSource; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }
}
