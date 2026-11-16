package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.DetectionProperties;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TimeoutRetryStormTest {
    private static final FailureSignalRegistry NO_DECLARATIONS = new FailureSignalRegistry("");

    @Test
    void theDeclaredTimeoutWordingMakesTheStormStrong() {
        RcaCandidate r02 = r02(analyze(SagaStatus.IN_PROGRESS, repositoryDeclarations(), retries(
                "{\"attemptNumber\":1,\"lastException\":\"TimeoutException: read timed out\"}")));

        assertThat(r02.getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
        assertThat(String.join(" ", r02.getEvidence().stream().map(Object::toString).toList()))
                .contains("TimeoutException: read timed out");
    }

    @Test
    void withoutADeclarationTheWordIsNotReadAsEvidence() {
        RcaCandidate r02 = r02(analyze(SagaStatus.IN_PROGRESS, NO_DECLARATIONS, retries(
                "{\"attemptNumber\":1,\"lastException\":\"TimeoutException: read timed out\"}")));

        assertThat(r02.getStatus()).isEqualTo(CandidateStatus.WEAK_EVIDENCE);
    }

    @Test
    void theWordElsewhereInThePayloadIsNotEvidence() {
        RcaCandidate r02 = r02(analyze(SagaStatus.IN_PROGRESS, repositoryDeclarations(), retries(
                "{\"attemptNumber\":1,\"timeoutMs\":2000,\"lastException\":\"RuntimeException: "
                        + "Payment gateway unavailable (simulated failure)\"}")));

        assertThat(r02.getStatus()).isEqualTo(CandidateStatus.WEAK_EVIDENCE);
    }

    private static RcaCandidate r02(RcaResult result) {
        return result.getCandidates().stream().filter(c -> "R02".equals(c.getRuleId())).findFirst()
                .orElseThrow(() -> new AssertionError("no R02 candidate: " + result.getCandidates()));
    }

    private static SagaEvent[] retries(String payload) {
        return new SagaEvent[] {
                event("e-1", "ORDER_CREATED", "{}", 0),
                event("e-2", "PAYMENT_RETRY", payload, 1),
                event("e-3", "PAYMENT_RETRY", payload, 2),
                event("e-4", "PAYMENT_RETRY", payload, 3)};
    }

    private static RcaResult analyze(SagaStatus status, FailureSignalRegistry failureSignals,
            SagaEvent... events) {
        Anomaly burst = new Anomaly();
        burst.setSagaId("saga-1");
        burst.setAnomalyType(AnomalyType.RETRY_BURST);
        burst.setDetectedAt(events[events.length - 1].getTimestamp());
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(status);
        saga.setStartTime(events[0].getTimestamp());

        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        when(sagas.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(List.of(events));
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.findStandingBySagaId(anyString())).thenReturn(List.of(burst));
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        RuleRegistry rules = new RuleRegistry();
        rules.load();

        return new RcaService(sagas, eventRepo, anomalies, results, workflows, new ObjectMapper(),
                new RcaSelector(), rules, new DetectionProperties(), mock(AlertSink.class),
                new ExecutionProjectionService(), failureSignals, null).analyze("saga-1");
    }

    private static FailureSignalRegistry repositoryDeclarations() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        FailureSignalRegistry registry = new FailureSignalRegistry(root.resolve("declarations").toString());
        registry.load();
        return registry;
    }

    private static SagaEvent event(String id, String type, String payload, int second) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("saga-1");
        event.setSourceName("default");
        event.setEventId(id);
        event.setEventType(type);
        event.setPayload(payload);
        event.setTimestamp(Instant.parse("2026-01-01T00:00:00Z").plusSeconds(second));
        return event;
    }
}
