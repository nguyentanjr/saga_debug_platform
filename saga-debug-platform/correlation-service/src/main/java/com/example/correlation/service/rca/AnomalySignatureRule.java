package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.rca.CandidateStatus;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AnomalySignatureRule {
    private String id;
    private String rootCause;
    private String severity;
    private CandidateStatus evidence = CandidateStatus.STRONG_EVIDENCE;

    private List<AnomalyType> matchesAnyAnomaly = new ArrayList<>();

    private List<String> evidenceNotes = new ArrayList<>();

    private List<String> suggestions = new ArrayList<>();

    public List<Anomaly> matching(List<Anomaly> anomalies) {
        if (anomalies == null || matchesAnyAnomaly.isEmpty()) {
            return List.of();
        }
        Set<AnomalyType> wanted = Set.copyOf(matchesAnyAnomaly);
        return anomalies.stream()
                .filter(anomaly -> anomaly != null && wanted.contains(anomaly.getAnomalyType()))
                .sorted(CITATION_ORDER)
                .toList();
    }

    private static final Comparator<Anomaly> CITATION_ORDER = Comparator
            .comparing(Anomaly::getMeasuredAt, Comparator.nullsLast(Comparator.naturalOrder()))
            .thenComparing(anomaly -> anomaly.getAnomalyType().name())
            .thenComparing(anomaly -> String.valueOf(anomaly.getObservedValue()))
            .thenComparing(anomaly -> String.valueOf(anomaly.getEvidenceEventIds()))
            .thenComparing(anomaly -> String.valueOf(anomaly.getDescription()));

    public Optional<String> validationError() {
        if (id == null || id.isBlank()) return Optional.of("no id");
        if (rootCause == null || rootCause.isBlank()) return Optional.of("no rootCause");
        if (severity == null || severity.isBlank()) return Optional.of("no severity");
        if (matchesAnyAnomaly.isEmpty()) {
            return Optional.of("matchesAnyAnomaly is empty, so the rule can never fire");
        }
        if (evidence == null) return Optional.of("no evidence tier");
        return Optional.empty();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getRootCause() { return rootCause; }
    public void setRootCause(String rootCause) { this.rootCause = rootCause; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public CandidateStatus getEvidence() { return evidence; }
    public void setEvidence(CandidateStatus evidence) { this.evidence = evidence; }

    public List<AnomalyType> getMatchesAnyAnomaly() { return matchesAnyAnomaly; }
    public void setMatchesAnyAnomaly(List<AnomalyType> matchesAnyAnomaly) {
        this.matchesAnyAnomaly = matchesAnyAnomaly != null
                ? new ArrayList<>(new LinkedHashSet<>(matchesAnyAnomaly))
                : new ArrayList<>();
    }

    public List<String> getEvidenceNotes() { return evidenceNotes; }
    public void setEvidenceNotes(List<String> evidenceNotes) {
        this.evidenceNotes = evidenceNotes != null ? evidenceNotes : new ArrayList<>();
    }

    public List<String> getSuggestions() { return suggestions; }
    public void setSuggestions(List<String> suggestions) {
        this.suggestions = suggestions != null ? suggestions : new ArrayList<>();
    }
}
