package com.example.correlation.controller;

import com.example.correlation.domain.*;
import com.example.correlation.dto.SagaDetailResponse;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.dto.SagaListItem;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/sagas")
public class SagaController {
    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final com.example.correlation.repository.ExecutionAliasRepository aliasRepository;
    private final ExecutionLookup executionLookup;

    public SagaController(SagaInstanceRepository sagaInstanceRepository,
                          SagaEventRepository sagaEventRepository,
                          AnomalyRepository anomalyRepository,
                          RcaResultRepository rcaResultRepository,
                          com.example.correlation.repository.ExecutionAliasRepository aliasRepository,
                          ExecutionLookup executionLookup,
                          com.example.correlation.workflow.WorkflowRegistry workflowRegistry) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.aliasRepository = aliasRepository;
        this.executionLookup = executionLookup;
        this.workflowRegistry = workflowRegistry;
    }

    private final com.example.correlation.workflow.WorkflowRegistry workflowRegistry;

    private com.example.correlation.repository.ServiceLifecycleEventRepository lifecycleRepository;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    void setLifecycleRepository(com.example.correlation.repository.ServiceLifecycleEventRepository repository) {
        this.lifecycleRepository = repository;
    }

    @GetMapping
    public ResponseEntity<Page<SagaListItem>> listSagas(
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "startTime", required = false) Instant startTime,
            @RequestParam(value = "endTime", required = false) Instant endTime,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size) {
        PageRequest pageRequest = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "startTime"));

        Page<SagaInstance> sagas;
        if (status != null && startTime != null && endTime != null) {
            sagas = sagaInstanceRepository.findByStatusAndStartTimeBetween(
                    SagaStatus.valueOf(status), startTime, endTime, pageRequest);
        } else if (status != null) {
            sagas = sagaInstanceRepository.findByStatus(SagaStatus.valueOf(status), pageRequest);
        } else if (startTime != null && endTime != null) {
            sagas = sagaInstanceRepository.findByStartTimeBetween(startTime, endTime, pageRequest);
        } else {
            sagas = sagaInstanceRepository.findAll(pageRequest);
        }

        List<String> sagaIds = sagas.getContent().stream()
                .map(SagaInstance::getSagaId)
                .collect(Collectors.toList());
        Map<String, Long> eventCounts = sagaIds.isEmpty() ? Map.of()
                : sagaEventRepository.countBySagaIdIn(sagaIds).stream()
                    .collect(Collectors.toMap(
                            r -> (String) r[0],
                            r -> (Long) r[1]));

        Map<String, RcaResult> latestRca = new java.util.HashMap<>();
        Map<String, Long> anomalyCounts = new java.util.HashMap<>();
        if (!sagaIds.isEmpty()) {
            for (RcaResult rca : rcaResultRepository.findBySagaIdInOrderByCreatedAtDesc(sagaIds)) {
                latestRca.putIfAbsent(rca.getSagaId(), rca);
            }
            for (Anomaly anomaly : anomalyRepository.findBySagaIdIn(sagaIds)) {
                if (anomaly.getRetractedAt() == null) {
                    anomalyCounts.merge(anomaly.getSagaId(), 1L, Long::sum);
                }
            }
        }

        Page<SagaListItem> result = sagas.map(saga -> {
            SagaListItem item = new SagaListItem();
            item.setSagaId(saga.getSagaId());
            item.setWorkflowType(saga.getWorkflowType());
            item.setWorkflowConflicts(saga.getWorkflowConflicts());
            item.setStatus(saga.getStatus());
            item.setStartTime(saga.getStartTime());
            item.setEndTime(saga.getEndTime());
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                item.setDurationMs(Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis());
            }
            item.setEventCount(eventCounts.getOrDefault(saga.getSagaId(), 0L));
            item.setAnomalyCount(anomalyCounts.getOrDefault(saga.getSagaId(), 0L));

            RcaResult rca = saga.hasWorkflowConflict() ? null : latestRca.get(saga.getSagaId());
            if (rca != null) {
                item.setRcaStatus(rca.getRcaStatus() != null ? rca.getRcaStatus().name() : null);
                item.setRootCause(rca.getRootCause());
                item.setRootCauseStep(promotedAnchorStep(rca));
            }
            return item;
        });

        return ResponseEntity.ok(result);
    }

    List<SagaDetailResponse.LifecycleEntry> lifecycleFor(SagaInstance saga,
            com.example.correlation.workflow.WorkflowSpec workflow) {
        if (lifecycleRepository == null || saga.getStartTime() == null) return List.of();
        java.util.Set<String> services = new java.util.LinkedHashSet<>();
        java.util.stream.Stream.concat(
                        workflow.getSteps() == null ? java.util.stream.Stream.empty() : workflow.getSteps().stream(),
                        workflow.getCompensationSteps() == null ? java.util.stream.Stream.empty()
                                : workflow.getCompensationSteps().stream())
                .map(step -> step.getService())
                .filter(service -> service != null && !service.isBlank())
                .forEach(services::add);
        if (services.isEmpty()) return List.of();

        Instant start = saga.getStartTime();
        Instant end = saga.getEndTime() != null ? saga.getEndTime() : Instant.now();
        List<com.example.correlation.domain.ServiceLifecycleEvent> found =
                com.example.correlation.service.rca.ServiceDownRule.readable(lifecycleRepository
                        .findByServiceNameInAndOccurredAtBetweenOrderByOccurredAtAsc(services,
                                start.minus(Duration.ofHours(1)), end), workflow.getRuntimeScope());

        Map<String, com.example.correlation.domain.ServiceLifecycleEvent> stateAtStart = new java.util.HashMap<>();
        List<com.example.correlation.domain.ServiceLifecycleEvent> shown = new java.util.ArrayList<>();
        for (var event : found) {
            if (event.getOccurredAt().isBefore(start)) stateAtStart.put(event.getServiceName(), event);
            else shown.add(event);
        }
        shown.addAll(stateAtStart.values());
        return shown.stream()
                .sorted(java.util.Comparator.comparing(com.example.correlation.domain.ServiceLifecycleEvent::getOccurredAt))
                .map(e -> new SagaDetailResponse.LifecycleEntry(e.getServiceName(), e.getState().name(),
                        e.getOccurredAt(), e.getSource(), e.getBasis().name(), e.getDetail()))
                .toList();
    }

    static String promotedAnchorStep(RcaResult rca) {
        if (rca.getRootCause() == null || rca.getCandidates() == null) return null;
        return rca.getCandidates().stream()
                .filter(c -> rca.getRootCause().equals(c.getRootCause())
                        && java.util.Objects.equals(rca.getRuleId(), c.getRuleId()))
                .map(com.example.correlation.domain.rca.RcaCandidate::getAnchorStepId)
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaDetailResponse> getSagaDetail(@PathVariable("sagaId") String sagaId) {
        String executionId = executionLookup.resolve(sagaId).orElse(null);
        if (executionId == null) {
            return ResponseEntity.notFound().build();
        }
        SagaInstance saga = sagaInstanceRepository.findById(executionId).orElse(null);
        if (saga == null) {
            return ResponseEntity.notFound().build();
        }

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(executionId);
        List<Anomaly> anomalies = anomalyRepository.findBySagaId(executionId);
        List<RcaResult> rcaResults =
                rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(executionId);

        SagaDetailResponse response = new SagaDetailResponse();
        response.setSagaId(saga.getSagaId());
        response.setSourceName(saga.getSourceName());

        response.setIdentities(aliasRepository.findByExecutionId(executionId).stream()
                .map(alias -> new SagaDetailResponse.IdentityLink(
                        alias.getAliasKey(), alias.getSourceName(), alias.getLinkedByEventId()))
                .toList());
        response.setWorkflowType(saga.getWorkflowType());
        response.setWorkflowConflicts(saga.getWorkflowConflicts());
        response.setStatus(saga.getStatus());
        response.setStartTime(saga.getStartTime());
        response.setEndTime(saga.getEndTime());
        if (saga.getStartTime() != null && saga.getEndTime() != null) {
            response.setDurationMs(Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis());
        }
        response.setEvents(events);
        if (!saga.hasWorkflowConflict()) {
            workflowRegistry.byType(saga.getWorkflowType())
                    .ifPresent(workflow -> response.setLifecycle(lifecycleFor(saga, workflow)));
            workflowRegistry.byType(saga.getWorkflowType()).ifPresent(workflow -> {
                java.util.Map<String, String> steps = new java.util.LinkedHashMap<>();
                for (SagaEvent event : events) {
                    if (event.getId() == null) continue;
                    workflow.classifyEvent(event.getEventType()).ifPresent(role ->
                            steps.put(String.valueOf(event.getId()), role.stepId()));
                }
                response.setWorkflowSteps(steps);
            });
        }
        if (saga.hasWorkflowConflict()) {
            anomalies.forEach(anomaly -> anomaly.setWorkflowConflicts(saga.getWorkflowConflicts()));
            rcaResults = List.of(RcaResult.workflowConflict(saga));
        }
        response.setAnomalies(anomalies);
        response.setRcaResults(rcaResults);

        return ResponseEntity.ok(response);
    }
}
