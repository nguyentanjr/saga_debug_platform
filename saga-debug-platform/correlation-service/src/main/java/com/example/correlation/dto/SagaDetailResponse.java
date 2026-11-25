package com.example.correlation.dto;

import com.example.correlation.domain.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class SagaDetailResponse {
    private String sagaId;

    private String sourceName;

    private List<IdentityLink> identities;
    private String workflowType;
    private List<String> workflowConflicts = List.of();

    private Map<String, String> workflowSteps = Map.of();
    public Map<String, String> getWorkflowSteps() { return workflowSteps; }
    public void setWorkflowSteps(Map<String, String> steps) { workflowSteps = steps; }
    public List<String> getWorkflowConflicts() { return workflowConflicts; }
    public void setWorkflowConflicts(List<String> conflicts) { workflowConflicts = conflicts; }
    private SagaStatus status;
    private Instant startTime;
    private Instant endTime;
    private Long durationMs;
    private List<SagaEvent> events;
    private List<Anomaly> anomalies;
    private List<RcaResult> rcaResults;

    private List<LifecycleEntry> lifecycle = List.of();

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public List<IdentityLink> getIdentities() { return identities; }
    public void setIdentities(List<IdentityLink> identities) { this.identities = identities; }

    public record LifecycleEntry(String service, String state, Instant occurredAt,
            String source, String basis, String detail) {}

    public List<LifecycleEntry> getLifecycle() { return lifecycle; }
    public void setLifecycle(List<LifecycleEntry> lifecycle) {
        this.lifecycle = lifecycle != null ? lifecycle : List.of();
    }

    public record IdentityLink(String identifier, String source, String linkedByEventId) {
    }

    public String getWorkflowType() { return workflowType; }
    public void setWorkflowType(String workflowType) { this.workflowType = workflowType; }

    public SagaStatus getStatus() { return status; }
    public void setStatus(SagaStatus status) { this.status = status; }

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }

    public List<SagaEvent> getEvents() { return events; }
    public void setEvents(List<SagaEvent> events) { this.events = events; }

    public List<Anomaly> getAnomalies() { return anomalies; }
    public void setAnomalies(List<Anomaly> anomalies) { this.anomalies = anomalies; }

    public List<RcaResult> getRcaResults() { return rcaResults; }
    public void setRcaResults(List<RcaResult> rcaResults) { this.rcaResults = rcaResults; }
}
