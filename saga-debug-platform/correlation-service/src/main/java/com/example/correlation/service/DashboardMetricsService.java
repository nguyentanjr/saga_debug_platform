package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.dto.DashboardSummaryResponse;
import com.example.correlation.dto.MetricItem;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DashboardMetricsService {
    private final SagaInstanceRepository sagaInstanceRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final SagaEventRepository sagaEventRepository;
    private final WorkflowRegistry workflowRegistry;

    public DashboardMetricsService(SagaInstanceRepository sagaInstanceRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository,
            SagaEventRepository sagaEventRepository,
            WorkflowRegistry workflowRegistry) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.workflowRegistry = workflowRegistry;
    }

    public DashboardSummaryResponse summarize(int requestedWindowHours) {
        int windowHours = requestedWindowHours > 0 ? requestedWindowHours : 24;
        Instant since = Instant.now().minus(windowHours, ChronoUnit.HOURS);

        List<Anomaly> anomalies = anomalyRepository.findByDetectedAtAfter(since).stream()
                .filter(anomaly -> anomaly.getRetractedAt() == null)
                .toList();
        List<RcaResult> rcaResults = rcaResultRepository.findByCreatedAtAfter(since);

        Set<String> ids = new HashSet<>();
        anomalies.forEach(a -> ids.add(a.getSagaId()));
        rcaResults.forEach(r -> ids.add(r.getSagaId()));
        Set<String> conflicted = new HashSet<>();
        List<SagaInstance> sagas = sagaInstanceRepository.findAllById(ids);
        sagas.stream()
                .filter(SagaInstance::hasWorkflowConflict)
                .forEach(saga -> conflicted.add(saga.getSagaId()));
        anomalies = anomalies.stream().filter(a -> !conflicted.contains(a.getSagaId())).toList();
        rcaResults = rcaResults.stream().filter(r -> !conflicted.contains(r.getSagaId())).toList();

        long activeSagas = countStatusSince(SagaStatus.STARTED, since)
                + countStatusSince(SagaStatus.IN_PROGRESS, since)
                + countStatusSince(SagaStatus.COMPENSATING, since);

        return new DashboardSummaryResponse(
                windowHours,
                since,
                sagaInstanceRepository.countByStartTimeAfter(since),
                countStatusSince(SagaStatus.COMPLETED, since),
                countStatusSince(SagaStatus.FAILED, since),
                countStatusSince(SagaStatus.COMPENSATED, since),
                activeSagas,
                anomalies.size(),
                countEvidenceAvailable(rcaResults),
                countRcaStatus(rcaResults, RcaStatus.INSUFFICIENT_EVIDENCE),
                countRcaStatus(rcaResults, RcaStatus.NO_DIAGNOSIS_NEEDED),
                topAnomalyTypes(anomalies),
                topRootCauses(rcaResults),
                topAffectedSteps(anomalies, sagas));
    }

    private long countStatusSince(SagaStatus status, Instant since) {
        return sagaInstanceRepository.countUncontestedByStatusSince(status.name(), since);
    }

    private long countRcaStatus(List<RcaResult> rcaResults, RcaStatus status) {
        return rcaResults.stream()
                .filter(result -> result.getRcaStatus() == status)
                .count();
    }

    private long countEvidenceAvailable(List<RcaResult> rcaResults) {
        return rcaResults.stream()
                .filter(result -> result.getRcaStatus() == RcaStatus.EVIDENCE_AVAILABLE)
                .count();
    }

    private List<MetricItem> topAnomalyTypes(List<Anomaly> anomalies) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Anomaly anomaly : anomalies) {
            counts.merge(anomaly.getAnomalyType().name(), 1L, Long::sum);
        }
        return topItems(counts, 5);
    }

    private List<MetricItem> topRootCauses(List<RcaResult> results) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (RcaResult result : results) {
            if (result.getRcaStatus() != RcaStatus.EVIDENCE_AVAILABLE) {
                continue;
            }
            List<String> roots = concreteEvidenceRootCauses(result);
            if (roots.isEmpty()) {
                if (result.getRootCause() != null && !result.getRootCause().isBlank()) {
                    counts.merge(result.getRootCause(), 1L, Long::sum);
                }
                continue;
            }
            for (String root : roots) {
                if (root != null && !root.isBlank()) {
                    counts.merge(root, 1L, Long::sum);
                }
            }
        }
        return topItems(counts, 5);
    }

    private List<String> concreteEvidenceRootCauses(RcaResult result) {
        if (result.getCandidates() != null && !result.getCandidates().isEmpty()) {
            return result.getCandidates().stream()
                    .filter(candidate -> candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE
                            || candidate.getStatus() == CandidateStatus.SUPPORTING_EVIDENCE)
                    .map(RcaCandidate::getRootCause)
                    .filter(root -> root != null && !root.isBlank())
                    .distinct()
                    .toList();
        }

        List<String> roots = result.getPrimaryRootCauses();
        return roots != null ? roots : List.of();
    }

    private List<MetricItem> topAffectedSteps(List<Anomaly> anomalies, List<SagaInstance> sagas) {
        Map<String, WorkflowSpec> workflowBySaga = new LinkedHashMap<>();
        for (SagaInstance saga : sagas) {
            if (saga.hasWorkflowConflict()) continue;
            workflowRegistry.byType(saga.getWorkflowType())
                    .ifPresent(spec -> workflowBySaga.put(saga.getSagaId(), spec));
        }
        List<String> sagaIds = anomalies.stream().map(Anomaly::getSagaId).distinct()
                .filter(workflowBySaga::containsKey).toList();
        Map<String, Map<String, SagaEvent>> eventsBySaga = sagaIds.isEmpty() ? Map.of()
                : sagaEventRepository.findBySagaIdInOrderBySagaIdAscTimestampAsc(sagaIds).stream()
                        .filter(e -> e.getEventId() != null)
                        .collect(Collectors.groupingBy(SagaEvent::getSagaId,
                                Collectors.toMap(SagaEvent::getEventId, e -> e, (a, b) -> a)));

        Map<String, Long> counts = new LinkedHashMap<>();
        for (Anomaly anomaly : anomalies) {
            WorkflowSpec workflow = workflowBySaga.get(anomaly.getSagaId());
            if (workflow == null) continue;
            affectedStep(anomaly, workflow, eventsBySaga.getOrDefault(anomaly.getSagaId(), Map.of()))
                    .ifPresent(step -> counts.merge(step, 1L, Long::sum));
        }
        return topItems(counts, 5);
    }

    private static Optional<String> affectedStep(Anomaly anomaly, WorkflowSpec workflow,
            Map<String, SagaEvent> eventsById) {
        String source = anomaly.getThresholdSource();
        if (source != null && source.startsWith("workflow:") && source.endsWith(".timeout")) {
            String step = source.substring("workflow:".length(), source.length() - ".timeout".length());
            if (workflow.hasStep(step)) return Optional.of(step);
        }
        List<String> cited = anomaly.getEvidenceEventIds() != null ? anomaly.getEvidenceEventIds() : List.of();
        Set<String> steps = cited.stream()
                .map(eventsById::get)
                .filter(java.util.Objects::nonNull)
                .map(e -> workflow.classifyEvent(e.getEventType())
                        .map(WorkflowSpec.EventRole::stepId).orElse(null))
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toSet());
        return steps.size() == 1 ? Optional.of(steps.iterator().next()) : Optional.empty();
    }

    private List<MetricItem> topItems(Map<String, Long> counts, int limit) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(limit)
                .map(entry -> new MetricItem(entry.getKey(), entry.getValue()))
                .toList();
    }
}
