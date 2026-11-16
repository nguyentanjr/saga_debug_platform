package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.DetectionProperties;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.alert.AlertEvent;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlertContentTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final List<AlertEvent> alerts = new ArrayList<>();
    private final List<Anomaly> anomalies = new ArrayList<>();

    @Test
    void aRunWhoseOnlyEvidenceIsAboutTheObservationProcessRaisesNoAlert() {
        anomalies.add(anomaly(1L, AnomalyType.OUT_OF_ORDER));

        RcaResult result = analyze(SagaStatus.COMPLETED,
                event("e-1", "ORDER_CREATED", 0), event("e-2", "INVENTORY_RESERVED", 1));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.NO_DIAGNOSIS_NEEDED);
        assertThat(result.getPrimaryRootCauses()).isEmpty();
        assertThat(alerts).isEmpty();
    }

    @Test
    void theAlertNamesExactlyTheCausesTheStoredResultNames() {
        anomalies.add(anomaly(1L, AnomalyType.OUT_OF_ORDER));
        anomalies.add(anomaly(2L, AnomalyType.MULTI_TERMINAL_CONFLICT));

        RcaResult result = analyze(SagaStatus.FAILED,
                event("e-1", "ORDER_CREATED", 0), event("e-2", "INVENTORY_RESERVED", 1));

        assertThat(result.getPrimaryRootCauses()).containsExactly("MULTI_TERMINAL_CONFLICT");
        assertThat(alerts).singleElement()
                .satisfies(alert -> assertThat(alert.primaryRootCauses())
                        .isEqualTo(result.getPrimaryRootCauses()));
    }

    @Test
    void silenceAloneIsAHypothesisAndAnnouncesNothing() {
        anomalies.add(anomaly(1L, AnomalyType.SAGA_STUCK));
        anomalies.add(anomaly(2L, AnomalyType.MISSING_TERMINAL_EVENT));

        RcaResult result = analyze(SagaStatus.IN_PROGRESS, event("e-1", "ORDER_CREATED", 0));

        assertThat(result.getCandidates()).extracting(c -> c.getRuleId() + ":" + c.getStatus())
                .contains("R05:WEAK_EVIDENCE");
        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.INSUFFICIENT_EVIDENCE);
        assertThat(result.getPrimaryRootCauses()).isEmpty();
        assertThat(alerts).isEmpty();
    }

    @Test
    void aStepsOwnDeclaredFailureStillAlerts() {
        analyze(SagaStatus.FAILED,
                event("e-1", "ORDER_CREATED", 0), event("e-2", "PAYMENT_FAILED", 1));

        assertThat(alerts).singleElement()
                .satisfies(alert -> assertThat(alert.primaryRootCauses())
                        .containsExactly("STEP_REPORTED_FAILURE"));
    }

    private RcaResult analyze(SagaStatus status, SagaEvent... events) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(status);
        saga.setStartTime(T0);
        saga.setLastUpdated(events[events.length - 1].getTimestamp());

        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        when(sagas.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(List.of(events));
        AnomalyRepository anomalyRepo = mock(AnomalyRepository.class);
        when(anomalyRepo.findStandingBySagaId(anyString())).thenReturn(anomalies);
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        RuleRegistry rules = new RuleRegistry();
        rules.load();

        return new RcaService(sagas, eventRepo, anomalyRepo, results, workflows, new ObjectMapper(),
                new RcaSelector(), rules, new DetectionProperties(), alerts::add,
                new ExecutionProjectionService(), new FailureSignalRegistry(""), null)
                .analyze("saga-1");
    }

    private static Anomaly anomaly(Long id, AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setAnomalyType(type);
        anomaly.setSagaId("saga-1");
        anomaly.setDescription(type.name());

        anomaly.setDetectedAt(T0.plusSeconds(3600));
        return anomaly;
    }

    private static SagaEvent event(String id, String type, int second) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("saga-1");
        event.setSourceName("default");
        event.setEventId(id);
        event.setEventType(type);
        event.setPayload("{}");
        event.setTimestamp(T0.plusSeconds(second));

        event.setReceivedAt(T0.plusSeconds(second));
        return event;
    }
}
