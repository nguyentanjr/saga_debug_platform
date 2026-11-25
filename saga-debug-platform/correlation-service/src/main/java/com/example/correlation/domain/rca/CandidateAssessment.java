package com.example.correlation.domain.rca;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CandidateAssessment {
    private String ruleId;
    private String rootCause;
    private String severity;
    private CandidateStatus status;
    private List<Evidence> evidence = new ArrayList<>();
    private List<String> suggestions = new ArrayList<>();
    private String statusReason;

    private ClaimSubject subject = ClaimSubject.OBSERVED_SYSTEM;

    private String anchorStepId;
    private Instant anchorAt;
    private List<String> precededBy = new ArrayList<>();

    public static Builder builder() {
        return new Builder();
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

    public Instant getAnchorAt() { return anchorAt; }
    public void setAnchorAt(Instant anchorAt) { this.anchorAt = anchorAt; }

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

    public static class Builder {
        private final CandidateAssessment candidate = new CandidateAssessment();

        public Builder ruleId(String ruleId) {
            candidate.setRuleId(ruleId);
            return this;
        }

        public Builder rootCause(String rootCause) {
            candidate.setRootCause(rootCause);
            return this;
        }

        public Builder severity(String severity) {
            candidate.setSeverity(severity);
            return this;
        }

        public Builder status(CandidateStatus status) {
            candidate.setStatus(status);
            return this;
        }

        public Builder evidence(Evidence evidence) {
            if (evidence != null) {
                candidate.evidence.add(evidence);
            }
            return this;
        }

        public Builder evidence(List<Evidence> evidence) {
            candidate.setEvidence(evidence);
            return this;
        }

        public Builder suggestion(String suggestion) {
            if (suggestion != null) {
                candidate.suggestions.add(suggestion);
            }
            return this;
        }

        public Builder suggestions(List<String> suggestions) {
            candidate.setSuggestions(suggestions);
            return this;
        }

        public Builder statusReason(String statusReason) {
            candidate.setStatusReason(statusReason);
            return this;
        }

        public Builder subject(ClaimSubject subject) {
            candidate.setSubject(subject);
            return this;
        }

        public CandidateAssessment build() {
            return candidate;
        }
    }
}
