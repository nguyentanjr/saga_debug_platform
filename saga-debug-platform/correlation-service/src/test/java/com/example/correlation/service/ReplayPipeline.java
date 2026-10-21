package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.*;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * The real correlation, inline detectors, RCA rules and selector over in-memory
 * repositories, for paired ablation inputs (E2, E3). No Kafka, no database, no scheduled
 * sweeps unless a test calls a detector pass itself.
 */
final class ReplayPipeline {

    static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();


    final Map<String, SagaInstance> instances = new LinkedHashMap<>();
    final List<SagaEvent> events = new ArrayList<>();
    final List<Anomaly> anomalies = new ArrayList<>();
    boolean retractionEnabled = true;
    final AnomalyDetectionService detector;
    final CorrelationService correlation;
    final RcaService rca;
    final SagaInstanceRepository sagas;
    final SagaEventRepository eventRepo;
    final AnomalyRepository anomalyRepo;
    final RcaResultRepository rcaRepo;

    ReplayPipeline() {
        this(null);
    }

    /** @param workflows the specifications to judge against; null loads the shipped ones */
    ReplayPipeline(WorkflowRegistry workflows) {
        sagas = mock(SagaInstanceRepository.class);
        eventRepo = mock(SagaEventRepository.class);
        anomalyRepo = mock(AnomalyRepository.class);
        rcaRepo = mock(RcaResultRepository.class);
        when(sagas.findById(anyString())).thenAnswer(c -> Optional.ofNullable(instances.get(c.getArgument(0))));
        when(sagas.findByIdForUpdate(anyString())).thenAnswer(c -> Optional.ofNullable(instances.get(c.getArgument(0))));
        when(sagas.save(any())).thenAnswer(c -> {
            SagaInstance s = c.getArgument(0); instances.put(s.getSagaId(), s); return s;
        });
        when(sagas.findByStatusIn(any(), any())).thenAnswer(c -> {
            Collection<SagaStatus> st = c.getArgument(0);
            return instances.values().stream().filter(s -> st.contains(s.getStatus())).toList();
        });
        when(eventRepo.findBySourceNameAndEventId(anyString(), anyString())).thenAnswer(c ->
                events.stream().filter(e -> e.getSourceName().equals(c.getArgument(0))
                        && e.getEventId().equals(c.getArgument(1))).findFirst());
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenAnswer(c ->
                events.stream().filter(e -> e.getSagaId().equals(c.getArgument(0)))
                        .sorted(Comparator.comparing(SagaEvent::getTimestamp)).toList());
        when(eventRepo.findTopBySagaIdOrderByTimestampDesc(anyString())).thenAnswer(c ->
                events.stream().filter(e -> e.getSagaId().equals(c.getArgument(0)))
                        .max(Comparator.comparing(SagaEvent::getTimestamp)));
        when(eventRepo.save(any())).thenAnswer(c -> {
            SagaEvent e = c.getArgument(0); events.add(e); return e;
        });
        when(anomalyRepo.save(any())).thenAnswer(c -> {
            Anomaly a = c.getArgument(0);
            // The ablated arm withholds every withdrawal, including those saved row by row.
            if (!retractionEnabled) { a.setRetractedAt(null); a.setRetractionReason(null); }
            if (a.getId() == null) { a.setId((long) anomalies.size() + 1); anomalies.add(a); }
            return a;
        });
        when(anomalyRepo.existsBySagaIdAndAnomalyType(anyString(), any())).thenAnswer(c ->
                anomalies.stream().anyMatch(a -> a.getSagaId().equals(c.getArgument(0))
                        && a.getAnomalyType() == c.getArgument(1) && a.getRetractedAt() == null));
        when(anomalyRepo.findStandingBySagaId(anyString())).thenAnswer(c ->
                anomalies.stream().filter(a -> a.getSagaId().equals(c.getArgument(0))
                        && a.getRetractedAt() == null).toList());
        when(anomalyRepo.findBySagaId(anyString())).thenAnswer(c ->
                anomalies.stream().filter(a -> a.getSagaId().equals(c.getArgument(0))).toList());
        when(anomalyRepo.retractClaims(anyString(), any(), any(), anyString())).thenAnswer(c -> {
            if (!retractionEnabled) return 0;
            Collection<AnomalyType> types = c.getArgument(1);
            int n = 0;
            for (Anomaly a : anomalies) {
                if (a.getSagaId().equals(c.getArgument(0)) && types.contains(a.getAnomalyType())
                        && a.getRetractedAt() == null) {
                    a.setRetractedAt(c.getArgument(2)); a.setRetractionReason(c.getArgument(3)); n++;
                }
            }
            return n;
        });
        when(rcaRepo.save(any())).thenAnswer(c -> c.getArgument(0));
        // What a fold of two executions does to stored rows (ExecutionAliasResolver.merge).
        when(eventRepo.repointSaga(anyString(), anyString())).thenAnswer(c -> {
            int n = 0;
            for (SagaEvent e : events) {
                if (e.getSagaId().equals(c.getArgument(0))) { e.setSagaId(c.getArgument(1)); n++; }
            }
            return n;
        });
        when(anomalyRepo.repointSaga(anyString(), anyString())).thenAnswer(c -> {
            int n = 0;
            for (Anomaly a : anomalies) {
                if (a.getSagaId().equals(c.getArgument(0))) { a.setSagaId(c.getArgument(1)); n++; }
            }
            return n;
        });
        doAnswer(c -> instances.remove(((SagaInstance) c.getArgument(0)).getSagaId()))
                .when(sagas).delete(any(SagaInstance.class));

        if (workflows == null) { workflows = new WorkflowRegistry(); workflows.load(); }
        RuleRegistry rules = new RuleRegistry(); rules.load();
        DetectionProperties properties = new DetectionProperties();
        ExecutionProjectionService projection = new ExecutionProjectionService();
        detector = new AnomalyDetectionService(sagas, eventRepo, anomalyRepo, workflows, MAPPER,
                properties, projection);
        correlation = new CorrelationService(sagas, eventRepo, detector, new DeduplicationCache(),
                workflows, MAPPER);
        FailureSignalRegistry failureSignals =
                new FailureSignalRegistry(CorrectnessAblationTest.repoRoot().resolve("declarations").toString());
        failureSignals.load();
        rca = new RcaService(sagas, eventRepo, anomalyRepo, rcaRepo, workflows, MAPPER,
                new RcaSelector(), rules, properties, mock(AlertSink.class), projection,
                failureSignals, null);
    }

    void deliver(List<IngestedEvent> batch) {
        batch.forEach(correlation::processEvent);
    }

    Outcome outcome(String executionId) {
        RcaResult r = rca.analyze(executionId);
        Outcome o = new Outcome();
        o.sagaStatus = instances.get(executionId).getStatus().name();
        o.rcaStatus = r.getRcaStatus().name();
        o.primary = r.getRootCause();
        o.candidates = r.getCandidates();
        o.candidateRules = r.getCandidates().stream()
                .map(c -> c.getRuleId() + ":" + c.getStatus()).toList();
        o.anomalies = anomalies.stream().filter(a -> a.getSagaId().equals(executionId))
                .filter(a -> a.getRetractedAt() == null)
                .map(a -> a.getAnomalyType().name()).sorted().toList();
        o.retracted = (int) anomalies.stream().filter(a -> a.getSagaId().equals(executionId))
                .filter(a -> a.getRetractedAt() != null).count();
        return o;
    }

static final class Outcome {
    String sagaStatus, rcaStatus, primary;
    List<String> anomalies, candidateRules;
    List<com.example.correlation.domain.rca.RcaCandidate> candidates;
    int retracted;

    @Override public String toString() {
        return "status=" + sagaStatus + " rca=" + rcaStatus + " primary=" + primary
                + " standing=" + anomalies + " candidates=" + candidateRules + " retracted=" + retracted;
    }
}
}
