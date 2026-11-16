package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
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

class CompensationAnchorTest {
    @Test
    void anUnacknowledgedCompensationOutranksTheDuplicateOfItsOwnRequest() {
        SagaEvent refund = event("e4", "REFUND_PAYMENT", "{}", 3);
        SagaEvent again = event("e5", "REFUND_PAYMENT", "{}", 4);
        RcaResult result = analyze(SagaStatus.COMPENSATING,
                List.of(anomaly(1, AnomalyType.MISSING_COMPENSATION_ACK, refund),
                        anomaly(2, AnomalyType.DUPLICATE_COMPENSATION, refund, again)),
                event("e1", "ORDER_CREATED", "{}", 0),
                event("e2", "PAYMENT_COMPLETED", "{}", 1),
                event("e3", "INVENTORY_FAILED", "{}", 2),
                refund, again);

        assertThat(result.getRuleId()).isEqualTo("R10");
        assertThat(result.getCandidates()).filteredOn(c -> "R10".equals(c.getRuleId()))
                .singleElement().satisfies(c -> assertThat(c.getAnchorAt()).isEqualTo(refund.getTimestamp()));
    }

    @Test
    void theForwardFailureOutranksTheSlownessOfTheRollbackThatAnsweredIt() {
        SagaEvent failed = event("e3", "INVENTORY_FAILED", "{}", 2);
        SagaEvent refund = event("e4", "REFUND_PAYMENT", "{}", 3);
        SagaEvent refunded = event("e5", "PAYMENT_REFUNDED", "{}", 23);
        RcaResult result = analyze(SagaStatus.COMPENSATED,
                List.of(anomaly(1, AnomalyType.STEP_TIMEOUT, refund, refunded),
                        anomaly(2, AnomalyType.DELAYED_COMPENSATION, refunded)),
                event("e1", "ORDER_CREATED", "{}", 0),
                event("e2", "PAYMENT_COMPLETED", "{}", 1),
                failed, refund, refunded);

        assertThat(result.getRuleId()).isEqualTo("R11");
        assertThat(result.getCandidates()).filteredOn(c -> "R11".equals(c.getRuleId()))
                .singleElement().satisfies(c -> assertThat(c.getAnchorAt()).isEqualTo(failed.getTimestamp()));
    }

    @Test
    void aFailureReportedAfterTheRunEndedIsNotTheStepsFailureClaim() {
        SagaEvent ended = event("e3", "INVENTORY_RESERVED", "{}", 2);
        SagaEvent late = event("e4", "PAYMENT_FAILED", "{}", 3);
        RcaResult result = analyze(SagaStatus.FAILED,
                List.of(anomaly(1, AnomalyType.MULTI_TERMINAL_CONFLICT, ended, late),
                        anomaly(2, AnomalyType.FORBIDDEN_POST_TERMINAL, ended, late)),
                event("e1", "ORDER_CREATED", "{}", 0),
                event("e2", "PAYMENT_COMPLETED", "{}", 1),
                ended, late);

        assertThat(result.getCandidates()).extracting(RcaCandidate::getRuleId).doesNotContain("R24");
        assertThat(result.getRuleId()).isEqualTo("R03");
    }

    @Test
    void aLateRollbackIsAnchoredAtTheRollbackNotAtTheFailureThatTriggeredIt() {
        SagaEvent failed = event("e3", "INVENTORY_FAILED", "{}", 4);
        SagaEvent refund = event("e4", "REFUND_PAYMENT", "{}", 39);
        SagaEvent refunded = event("e5", "PAYMENT_REFUNDED", "{}", 44);
        RcaResult result = analyze(SagaStatus.COMPENSATED,
                List.of(anomaly(1, AnomalyType.DELAYED_COMPENSATION, failed, refund)),
                event("e1", "ORDER_CREATED", "{}", 0),
                event("e2", "PAYMENT_COMPLETED", "{}", 2),
                failed, refund, refunded);

        assertThat(result.getRuleId()).isEqualTo("R11");
        assertThat(result.getCandidates()).filteredOn(c -> "R22".equals(c.getRuleId()))
                .singleElement().satisfies(c -> assertThat(c.getAnchorAt()).isEqualTo(refund.getTimestamp()));
    }

    @Test
    void aStepTimeoutIsAnchoredAtTheStepThatWasLate() {
        SagaEvent paid = event("e2", "PAYMENT_COMPLETED", "{}", 1);
        SagaEvent reserved = event("e3", "INVENTORY_RESERVED", "{}", 50);
        RcaResult result = analyze(SagaStatus.COMPLETED,
                List.of(anomaly(1, AnomalyType.STEP_TIMEOUT, paid, reserved)),
                event("e1", "ORDER_CREATED", "{}", 0), paid, reserved);

        assertThat(result.getCandidates()).filteredOn(c -> "R07".equals(c.getRuleId()))
                .singleElement().satisfies(c -> {
                    assertThat(c.getAnchorStepId()).isEqualTo("RESERVE_INVENTORY");
                    assertThat(c.getAnchorAt()).isEqualTo(reserved.getTimestamp());
                });
    }

    private static RcaResult analyze(SagaStatus status, List<Anomaly> standing, SagaEvent... events) {
        FailureSignalRegistry failureSignals = new FailureSignalRegistry("");
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
        when(anomalies.findStandingBySagaId(anyString())).thenReturn(standing);
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

    private static Anomaly anomaly(long id, AnomalyType type, SagaEvent... cited) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setSagaId("saga-1");
        anomaly.setAnomalyType(type);
        anomaly.setDetectedAt(cited[cited.length - 1].getTimestamp());
        anomaly.setMeasuredAt(cited[0].getTimestamp());
        anomaly.setEvidenceEventIds(java.util.Arrays.stream(cited).map(SagaEvent::getEventId).toList());
        return anomaly;
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
