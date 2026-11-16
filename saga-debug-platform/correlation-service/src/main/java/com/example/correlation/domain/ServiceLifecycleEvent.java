package com.example.correlation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "service_lifecycle_events")
public class ServiceLifecycleEvent {
    public enum State { DOWN, UP }

    public enum Basis { PROCESS, PROBE }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "service_name", nullable = false)
    private String serviceName;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false)
    private State state;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "source", nullable = false)
    private String source;

    @Column(name = "detail")
    private String detail;

    @Column(name = "external_id", nullable = false)
    private String externalId;

    @Enumerated(EnumType.STRING)
    @Column(name = "basis", nullable = false)
    private Basis basis = Basis.PROCESS;

    @Column(name = "scope")
    private String scope;

    protected ServiceLifecycleEvent() {
    }

    public ServiceLifecycleEvent(String serviceName, State state, Instant occurredAt,
            String source, String detail, String externalId) {
        this.serviceName = serviceName;
        this.state = state;
        this.occurredAt = occurredAt;
        this.receivedAt = Instant.now();
        this.source = source;
        this.detail = detail;
        this.externalId = externalId;
    }

    public ServiceLifecycleEvent(String serviceName, State state, Instant occurredAt,
            String source, String detail, String externalId, Basis basis) {
        this(serviceName, state, occurredAt, source, detail, externalId);
        this.basis = basis;
    }

    public Long getId() { return id; }
    public String getServiceName() { return serviceName; }
    public State getState() { return state; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getReceivedAt() { return receivedAt; }
    public String getSource() { return source; }
    public String getDetail() { return detail; }
    public String getExternalId() { return externalId; }
    public Basis getBasis() { return basis; }
    public String getScope() { return scope; }

    public ServiceLifecycleEvent inScope(String scope) {
        this.scope = scope == null || scope.isBlank() ? null : scope;
        return this;
    }
}
