package com.example.correlation.domain;

import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.List;

@Entity
@Table(name = "rca_results")
public class RcaResult {
    public static RcaResult workflowConflict(SagaInstance saga) {
        RcaResult result = new RcaResult();
        result.setSagaId(saga.getSagaId());
        result.setOutcome("UNDETERMINED");
        result.setSeverity("UNKNOWN");
        result.setRcaStatus(RcaStatus.INSUFFICIENT_EVIDENCE);
        result.setEvidence("[\"Workflow declarations conflict; diagnosis withheld\"]");
        result.setSuggestions("[\"Correct the conflicting workflow declarations before re-evaluating this execution\"]");
        result.setMissingEvidence(List.of("Unambiguous workflow required; conflicting declarations: "
                + String.join(", ", saga.getWorkflowConflicts())));
        return result;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false)
    private String sagaId;

    @Column(name = "root_cause")
    private String rootCause;

    @Column(name = "outcome", nullable = false)
    private String outcome;

    @Column(nullable = false)
    private String severity;

    @Column(name = "confidence")
    private Double confidence;

    @Column(name = "rule_id")
    private String ruleId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String evidence;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String suggestions;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "contributing_causes", columnDefinition = "jsonb")
    private String contributingCauses;

    @Enumerated(EnumType.STRING)
    @Column(name = "rca_status", nullable = false)
    private RcaStatus rcaStatus = RcaStatus.NO_DIAGNOSIS_NEEDED;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "primary_root_causes", nullable = false, columnDefinition = "jsonb")
    private List<String> primaryRootCauses = List.of();

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "candidates", nullable = false, columnDefinition = "jsonb")
    private List<RcaCandidate> candidates = List.of();

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "missing_evidence", nullable = false, columnDefinition = "jsonb")
    private List<String> missingEvidence = List.of();

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getRootCause() { return rootCause; }
    public void setRootCause(String rootCause) { this.rootCause = rootCause; }

    public String getOutcome() { return outcome; }
    public void setOutcome(String outcome) { this.outcome = outcome; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    @JsonIgnore
    public Double getConfidence() { return confidence; }
    public void setConfidence(Double confidence) { this.confidence = confidence; }

    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    public String getEvidence() { return evidence; }
    public void setEvidence(String evidence) { this.evidence = evidence; }

    public String getSuggestions() { return suggestions; }
    public void setSuggestions(String suggestions) { this.suggestions = suggestions; }

    @JsonIgnore
    public String getContributingCauses() { return contributingCauses; }
    public void setContributingCauses(String contributingCauses) { this.contributingCauses = contributingCauses; }

    public RcaStatus getRcaStatus() { return rcaStatus; }
    public void setRcaStatus(RcaStatus rcaStatus) { this.rcaStatus = rcaStatus; }

    public List<String> getPrimaryRootCauses() { return primaryRootCauses; }
    public void setPrimaryRootCauses(List<String> primaryRootCauses) {
        this.primaryRootCauses = primaryRootCauses != null ? primaryRootCauses : List.of();
    }

    public List<RcaCandidate> getCandidates() { return candidates; }
    public void setCandidates(List<RcaCandidate> candidates) {
        this.candidates = candidates != null ? candidates : List.of();
    }

    public List<String> getMissingEvidence() { return missingEvidence; }
    public void setMissingEvidence(List<String> missingEvidence) {
        this.missingEvidence = missingEvidence != null ? missingEvidence : List.of();
    }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
