package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.ClaimSubject;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class RcaSelector {
    private static final Set<String> FALLBACK_RULES = Set.of("R20", "R21");

    public SelectionResult select(List<CandidateAssessment> input,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        return select(input, anomalies, saga, ExecutionOrder.unknown());
    }

    public SelectionResult select(List<CandidateAssessment> input,
            List<Anomaly> anomalies,
            SagaInstance saga,
            ExecutionOrder order) {
        List<CandidateAssessment> candidates = input != null ? new ArrayList<>(input) : new ArrayList<>();
        List<Anomaly> anomalyList = anomalies != null ? anomalies : List.of();

        applyExclusions(candidates);
        recordPrecedence(candidates, order);
        candidates.sort(comparator(order));

        List<CandidateAssessment> evidenceCandidates = candidates.stream()
                .filter(c -> c.getStatus() != CandidateStatus.RULED_OUT)
                .collect(Collectors.toList());

        List<RcaCandidate> ranked = candidates.stream()
                .map(RcaCandidate::from)
                .collect(Collectors.toList());

        SelectionResult result = new SelectionResult();
        result.setRcaStatus(resolveStatus(evidenceCandidates, candidates, anomalyList, saga));
        result.setCandidates(ranked);
        result.setPrimary(promote(ranked));
        result.setMissingEvidence(buildMissingEvidence(candidates, result.getRcaStatus()));
        return result;
    }

    private RcaCandidate promote(List<RcaCandidate> ranked) {
        return ranked.stream()
                .filter(candidate -> candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE)
                .filter(candidate -> candidate.getSubject() == ClaimSubject.OBSERVED_SYSTEM)
                .findFirst()
                .orElse(null);
    }

    public interface ExecutionOrder {
        OptionalInt positionOf(String stepId);

        boolean isUpstream(String candidate, String step);

        static ExecutionOrder unknown() {
            return new ExecutionOrder() {
                @Override public OptionalInt positionOf(String stepId) { return OptionalInt.empty(); }
                @Override public boolean isUpstream(String candidate, String step) { return false; }
            };
        }
    }

    private void recordPrecedence(List<CandidateAssessment> candidates, ExecutionOrder order) {
        for (CandidateAssessment candidate : candidates) {
            String step = candidate.getAnchorStepId();
            if (step == null) continue;

            List<String> upstream = candidates.stream()
                    .filter(other -> other != candidate)
                    .filter(other -> other.getStatus() != CandidateStatus.RULED_OUT)
                    .filter(other -> other.getAnchorStepId() != null)
                    .filter(other -> order.isUpstream(other.getAnchorStepId(), step))
                    .map(CandidateAssessment::getRuleId)
                    .filter(Objects::nonNull)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());

            if (upstream.isEmpty()) continue;
            candidate.setPrecededBy(upstream);

            String note = "Anchored at " + step + ", downstream of " + upstream
                    + "; ordering only, no causal link established.";
            candidate.setStatusReason(candidate.getStatusReason() == null
                    ? note
                    : candidate.getStatusReason() + " " + note);
        }
    }

    private Comparator<CandidateAssessment> comparator(ExecutionOrder order) {
        return Comparator
                .comparingInt((CandidateAssessment c) -> statusRank(c.getStatus()))
                .thenComparingInt(c -> anchorPosition(c, order))
                .thenComparing(CandidateAssessment::getAnchorAt,
                        Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparingInt(c -> -severityRank(c.getSeverity()))
                .thenComparing(CandidateAssessment::getRuleId,
                        Comparator.nullsLast(Comparator.naturalOrder()));
    }

    private int anchorPosition(CandidateAssessment candidate, ExecutionOrder order) {
        if (candidate.getAnchorStepId() == null) return Integer.MAX_VALUE;
        return order.positionOf(candidate.getAnchorStepId()).orElse(Integer.MAX_VALUE);
    }

    private static int statusRank(CandidateStatus status) {
        if (status == CandidateStatus.STRONG_EVIDENCE) return 0;
        if (status == CandidateStatus.SUPPORTING_EVIDENCE) return 1;
        if (status == CandidateStatus.WEAK_EVIDENCE) return 2;
        if (status == CandidateStatus.RULED_OUT) return 3;
        return 4;
    }

    private static int severityRank(String severity) {
        if ("CRITICAL".equals(severity)) return 4;
        if ("HIGH".equals(severity)) return 3;
        if ("MEDIUM".equals(severity)) return 2;
        if ("LOW".equals(severity)) return 1;
        return 0;
    }

    private void applyExclusions(List<CandidateAssessment> candidates) {
        boolean hasSpecificEvidence = candidates.stream()
                .anyMatch(c -> !FALLBACK_RULES.contains(c.getRuleId())
                        && c.getSubject() == ClaimSubject.OBSERVED_SYSTEM
                        && c.getStatus() != CandidateStatus.RULED_OUT);
        if (!hasSpecificEvidence) {
            return;
        }
        for (CandidateAssessment candidate : candidates) {
            if (FALLBACK_RULES.contains(candidate.getRuleId())) {
                candidate.setStatus(CandidateStatus.RULED_OUT);
                candidate.setStatusReason("Specific evidence candidates are already available; "
                        + candidate.getRootCause() + " is fallback-only.");
            }
        }
    }

    private RcaStatus resolveStatus(List<CandidateAssessment> evidenceCandidates,
            List<CandidateAssessment> allCandidates,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        boolean hasActionableEvidence = evidenceCandidates.stream()
                .filter(candidate -> candidate.getSubject() == ClaimSubject.OBSERVED_SYSTEM)
                .anyMatch(candidate -> candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE
                        || candidate.getStatus() == CandidateStatus.SUPPORTING_EVIDENCE);
        if (hasActionableEvidence) {
            return RcaStatus.EVIDENCE_AVAILABLE;
        }
        Set<Long> onlyAboutObservation = citedAnomalies(allCandidates, ClaimSubject.OBSERVATION_PROCESS);
        onlyAboutObservation.removeAll(citedAnomalies(allCandidates, ClaimSubject.OBSERVED_SYSTEM));
        boolean hasAnomalies = anomalies != null && anomalies.stream()
                .anyMatch(anomaly -> anomaly.getId() == null || !onlyAboutObservation.contains(anomaly.getId()));
        if (!hasAnomalies && saga != null
                && (saga.getStatus() == SagaStatus.COMPLETED || saga.getStatus() == SagaStatus.FAILED)) {
            return RcaStatus.NO_DIAGNOSIS_NEEDED;
        }
        return RcaStatus.INSUFFICIENT_EVIDENCE;
    }

    private static Set<Long> citedAnomalies(List<CandidateAssessment> candidates, ClaimSubject subject) {
        return candidates.stream()
                .filter(candidate -> candidate.getSubject() == subject)
                .flatMap(candidate -> candidate.getEvidence().stream())
                .filter(Objects::nonNull)
                .map(Evidence::anomalyId)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(HashSet::new));
    }

    private List<String> buildMissingEvidence(List<CandidateAssessment> candidates, RcaStatus status) {
        if (status != RcaStatus.INSUFFICIENT_EVIDENCE) {
            return List.of();
        }
        List<String> missing = candidates.stream()
                .filter(c -> c.getStatus() == CandidateStatus.WEAK_EVIDENCE)
                .map(CandidateAssessment::getStatusReason)
                .filter(reason -> reason != null && !reason.isBlank())
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
        if (missing.isEmpty()) {
            missing.add("No RCA candidate could be built from the available evidence.");
        }
        return missing;
    }

    public static class SelectionResult {
        private RcaStatus rcaStatus;
        private List<RcaCandidate> candidates = new ArrayList<>();
        private List<String> missingEvidence = new ArrayList<>();

        private RcaCandidate primary;

        public RcaStatus getRcaStatus() { return rcaStatus; }
        public void setRcaStatus(RcaStatus rcaStatus) { this.rcaStatus = rcaStatus; }

        public RcaCandidate getPrimary() { return primary; }
        public void setPrimary(RcaCandidate primary) { this.primary = primary; }

        public List<RcaCandidate> getCandidates() { return candidates; }
        public void setCandidates(List<RcaCandidate> candidates) {
            this.candidates = candidates != null ? new ArrayList<>(candidates) : new ArrayList<>();
        }

        public List<String> getMissingEvidence() { return missingEvidence; }
        public void setMissingEvidence(List<String> missingEvidence) {
            this.missingEvidence = missingEvidence != null ? new ArrayList<>(missingEvidence) : new ArrayList<>();
        }
    }
}
