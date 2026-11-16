package com.example.correlation.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.List;

@Entity
@Table(name = "anomalies")
public class Anomaly {
    @Transient
    private List<String> workflowConflicts = List.of();

    public List<String> getWorkflowConflicts() { return workflowConflicts; }
    public void setWorkflowConflicts(List<String> conflicts) { workflowConflicts = List.copyOf(conflicts); }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false)
    private String sagaId;

    @Enumerated(EnumType.STRING)
    @Column(name = "anomaly_type", nullable = false)
    private AnomalyType anomalyType;

    @Column(name = "detected_at", nullable = false)
    private Instant detectedAt = Instant.now();

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "observed_value", columnDefinition = "TEXT")
    private String observedValue;

    @Column(name = "expected_value", columnDefinition = "TEXT")
    private String expectedValue;

    @Column(name = "threshold_source")
    private String thresholdSource;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "evidence_event_ids", columnDefinition = "jsonb")
    private List<String> evidenceEventIds;

    @Column(name = "measured_at")
    private Instant measuredAt;

    @Column(name = "retracted_at")
    private Instant retractedAt;

    @Column(name = "retraction_reason")
    private String retractionReason;

    public Instant getRetractedAt() { return retractedAt; }
    public void setRetractedAt(Instant retractedAt) { this.retractedAt = retractedAt; }

    public String getRetractionReason() { return retractionReason; }
    public void setRetractionReason(String retractionReason) {
        this.retractionReason = retractionReason;
    }

    public boolean isRetracted() { return retractedAt != null; }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public AnomalyType getAnomalyType() { return anomalyType; }
    public void setAnomalyType(AnomalyType anomalyType) { this.anomalyType = anomalyType; }

    public Instant getDetectedAt() { return detectedAt; }
    public void setDetectedAt(Instant detectedAt) { this.detectedAt = detectedAt; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getObservedValue() { return observedValue; }
    public void setObservedValue(String observedValue) { this.observedValue = observedValue; }

    public String getExpectedValue() { return expectedValue; }
    public void setExpectedValue(String expectedValue) { this.expectedValue = expectedValue; }

    public String getThresholdSource() { return thresholdSource; }
    public void setThresholdSource(String thresholdSource) { this.thresholdSource = thresholdSource; }

    public List<String> getEvidenceEventIds() { return evidenceEventIds; }
    public void setEvidenceEventIds(List<String> evidenceEventIds) { this.evidenceEventIds = evidenceEventIds; }

    public Instant getMeasuredAt() { return measuredAt; }
    public void setMeasuredAt(Instant measuredAt) { this.measuredAt = measuredAt; }
}
