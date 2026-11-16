package com.example.correlation.domain.rca;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RcaCandidate {
    private String ruleId;
    private String rootCause;
    private String severity;
    private CandidateStatus status;
    private List<Evidence> evidence = new ArrayList<>();
    private List<String> suggestions = new ArrayList<>();
    private String statusReason;

    private ClaimSubject subject = ClaimSubject.OBSERVED_SYSTEM;

    private String anchorStepId;

    private java.time.Instant anchorAt;

    private List<String> precededBy = new ArrayList<>();

    public static RcaCandidate from(CandidateAssessment assessment) {
        RcaCandidate candidate = new RcaCandidate();
        candidate.setRuleId(assessment.getRuleId());
        candidate.setRootCause(assessment.getRootCause());
        candidate.setSeverity(assessment.getSeverity());
        candidate.setStatus(assessment.getStatus());
        candidate.setEvidence(assessment.getEvidence());
        candidate.setSuggestions(assessment.getSuggestions());
        candidate.setStatusReason(assessment.getStatusReason());
        candidate.setSubject(assessment.getSubject());
        candidate.setAnchorStepId(assessment.getAnchorStepId());
        candidate.setAnchorAt(assessment.getAnchorAt());
        candidate.setPrecededBy(assessment.getPrecededBy());
        return candidate;
    }

    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    public String getRootCause() { return rootCause; }
    public void setRootCause(String rootCause) { this.rootCause = rootCause; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public CandidateStatus getStatus() { return status; }
    public void setStatus(CandidateStatus status) { this.status = status; }

    public List<Evidence> getEvidence() { return evidence; }
    public void setEvidence(List<Evidence> evidence) {
        this.evidence = evidence != null ? new ArrayList<>(evidence) : new ArrayList<>();
    }

    public List<String> getSuggestions() { return suggestions; }
    public void setSuggestions(List<String> suggestions) {
        this.suggestions = suggestions != null ? new ArrayList<>(suggestions) : new ArrayList<>();
    }

    public String getAnchorStepId() { return anchorStepId; }
    public void setAnchorStepId(String anchorStepId) { this.anchorStepId = anchorStepId; }

    public java.time.Instant getAnchorAt() { return anchorAt; }
    public void setAnchorAt(java.time.Instant anchorAt) { this.anchorAt = anchorAt; }

    public List<String> getPrecededBy() { return precededBy; }
    public void setPrecededBy(List<String> precededBy) {
        this.precededBy = precededBy != null ? new ArrayList<>(precededBy) : new ArrayList<>();
    }

    public String getStatusReason() { return statusReason; }
    public void setStatusReason(String statusReason) { this.statusReason = statusReason; }

    public ClaimSubject getSubject() { return subject; }
    public void setSubject(ClaimSubject subject) {
        this.subject = subject != null ? subject : ClaimSubject.OBSERVED_SYSTEM;
    }
}
