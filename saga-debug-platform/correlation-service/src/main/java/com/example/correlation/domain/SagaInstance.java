package com.example.correlation.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.List;
import java.util.TreeSet;

@Entity
@Table(name = "saga_instances")
public class SagaInstance {
    @Id
    @Column(name = "saga_id")
    private String sagaId;

    @Column(name = "source_name", nullable = false)
    private String sourceName = ExecutionKey.DEFAULT_SOURCE;

    public static final String UNRESOLVED = "UNRESOLVED";

    @Column(name = "workflow_type", nullable = false)
    private String workflowType = UNRESOLVED;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "workflow_conflicts", nullable = false, columnDefinition = "jsonb")
    private List<String> workflowConflicts = List.of();

    public List<String> getWorkflowConflicts() { return workflowConflicts; }
    public boolean hasWorkflowConflict() { return !workflowConflicts.isEmpty(); }

    public void recordWorkflowConflict(String candidate) {
        if (candidate == null || UNRESOLVED.equals(candidate)
                || workflowType == null || UNRESOLVED.equals(workflowType)) return;
        if (!hasWorkflowConflict() && workflowType.equals(candidate)) return;
        TreeSet<String> names = new TreeSet<>(workflowConflicts);
        names.add(workflowType);
        names.add(candidate);
        workflowConflicts = List.copyOf(names);
    }

    @Column(name = "order_id")
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SagaStatus status;

    @Column(name = "start_time", nullable = false)
    private Instant startTime;

    @Column(name = "end_time")
    private Instant endTime;

    @Column(name = "last_updated", nullable = false)
    private Instant lastUpdated;

    @Version
    @Column(nullable = false)
    private long version;

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public String getWorkflowType() { return workflowType; }
    public void setWorkflowType(String workflowType) { this.workflowType = workflowType; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public SagaStatus getStatus() { return status; }
    public void setStatus(SagaStatus status) { this.status = status; }

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public Instant getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(Instant lastUpdated) { this.lastUpdated = lastUpdated; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
