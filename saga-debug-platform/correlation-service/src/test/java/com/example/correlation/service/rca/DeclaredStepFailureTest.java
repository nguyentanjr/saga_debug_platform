package com.example.correlation.service.rca;

import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
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

class DeclaredStepFailureTest {
    private static final FailureSignalRegistry NO_DECLARATIONS = new FailureSignalRegistry("");

    @Test
    void aDeclaredFailedEventWithoutReadableReasonIsStatedAsTheStepsFailure() {
        RcaResult result = analyze(SagaStatus.FAILED, NO_DECLARATIONS,
                event("e-1", "ORDER_CREATED", "{}", 0),
                event("e-2", "PAYMENT_FAILED", "{\"status\":\"REJECTED\"}", 1));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.EVIDENCE_AVAILABLE);
        assertThat(result.getRootCause()).isEqualTo("STEP_REPORTED_FAILURE");
        assertThat(result.getRuleId()).isEqualTo("R24");
        assertThat(result.getOutcome()).isEqualTo("FAILED");
        assertThat(result.getEvidence()).contains("PAYMENT_FAILED").doesNotContain("completed successfully");
    }

    @Test
    void aReadableDeclaredSignalLeavesNoRoomForR24() {
        RcaResult result = analyze(SagaStatus.FAILED, repositoryDeclarations(),
                event("e-1", "ORDER_CREATED", "{}", 0),
                event("e-2", "PAYMENT_FAILED", "{\"reason\":\"Circuit breaker / retry exhausted: "
                        + "CircuitBreaker 'paymentProcessing' is OPEN and does not permit further calls\"}", 1));

        assertThat(result.getRuleId()).isEqualTo("R01");
        assertThat(result.getCandidates()).extracting(RcaCandidate::getRuleId).doesNotContain("R24");
    }

    @Test
    void aFailedRunWithoutEvidenceIsNotReportedAsSuccessful() {
        RcaResult result = analyze(SagaStatus.FAILED, NO_DECLARATIONS,
                event("e-1", "ORDER_CREATED", "{}", 0));

        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.NO_DIAGNOSIS_NEEDED);
        assertThat(result.getOutcome()).isEqualTo("FAILED");
        assertThat(result.getEvidence()).doesNotContain("completed successfully");
    }

    @Test
    void aFailedEventOnARunStillInProgressDoesNotFire() {
        RcaResult result = analyze(SagaStatus.IN_PROGRESS, NO_DECLARATIONS,
                event("e-1", "ORDER_CREATED", "{}", 0),
                event("e-2", "PAYMENT_FAILED", "{\"status\":\"REJECTED\"}", 1));

        assertThat(result.getCandidates()).extracting(RcaCandidate::getRuleId).doesNotContain("R24");
    }

    private static RcaResult analyze(SagaStatus status, FailureSignalRegistry failureSignals,
            SagaEvent... events) {
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
        when(anomalies.findStandingBySagaId(anyString())).thenReturn(List.of());
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
