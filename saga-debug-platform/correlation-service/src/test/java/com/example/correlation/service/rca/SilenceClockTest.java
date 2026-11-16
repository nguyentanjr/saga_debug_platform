package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.DetectionProperties;
import com.example.correlation.service.RcaService;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SilenceClockTest {
    private static final Instant HEARD = Instant.parse("2026-01-01T12:00:00Z");

    @Test
    void aSourceClockRunningBehindDoesNotInflateTheSilence() {
        RcaResult result = analyze(HEARD.minusSeconds(600), HEARD, HEARD.plusSeconds(20));

        assertThat(result.getCandidates()).extracting(RcaCandidate::getRuleId).doesNotContain("R05");
    }

    @Test
    void aSourceClockRunningAheadDoesNotHideTheSilence() {
        RcaResult result = analyze(HEARD.plusSeconds(600), HEARD, HEARD.plusSeconds(60));

        assertThat(result.getCandidates()).filteredOn(c -> "R05".equals(c.getRuleId()))
                .singleElement()
                .satisfies(c -> assertThat(c.getEvidence()).anyMatch(e -> e.fact().contains("60s")));
    }

    @Test
    void theRecoveredRunsTotalDurationSaysWhichClocksItSpans() {
        SagaEvent first = event("e-1", "ORDER_CREATED", HEARD, HEARD);
        first.setEventTimeSource(com.example.correlation.domain.EventTimeSource.DECLARED);
        SagaEvent last = event("e-2", "INVENTORY_RESERVED", HEARD.plusSeconds(90), HEARD.plusSeconds(90));
        last.setEventTimeSource(com.example.correlation.domain.EventTimeSource.TRANSPORT);
        SagaInstance saga = saga(SagaStatus.COMPLETED, HEARD);
        saga.setEndTime(HEARD.plusSeconds(90));

        RcaResult result = analyze(saga, List.of(first, last),
                List.of(anomaly(1L, AnomalyType.SAGA_STUCK, HEARD.plusSeconds(60))));

        assertThat(result.getCandidates()).filteredOn(c -> "R06".equals(c.getRuleId()))
                .singleElement()
                .satisfies(c -> assertThat(c.getEvidence()).anyMatch(e ->
                        e.fact().contains("90000ms") && e.fact().contains("clocks=DECLARED->TRANSPORT (mixed)")));
    }

    private static RcaResult analyze(Instant stamped, Instant received, Instant detected) {
        return analyze(saga(SagaStatus.IN_PROGRESS, stamped),
                List.of(event("e-1", "ORDER_CREATED", stamped, received)),
                List.of(anomaly(1L, AnomalyType.SAGA_STUCK, detected),
                        anomaly(2L, AnomalyType.MISSING_TERMINAL_EVENT, detected)));
    }

    private static SagaEvent event(String id, String type, Instant stamped, Instant received) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("saga-1");
        event.setSourceName("default");
        event.setEventId(id);
        event.setEventType(type);
        event.setPayload("{}");
        event.setTimestamp(stamped);
        event.setReceivedAt(received);
        return event;
    }

    private static SagaInstance saga(SagaStatus status, Instant start) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(status);
        saga.setStartTime(start);
        saga.setLastUpdated(start);
        return saga;
    }

    private static RcaResult analyze(SagaInstance saga, List<SagaEvent> events, List<Anomaly> standing) {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        when(sagas.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(events);
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.findStandingBySagaId(anyString())).thenReturn(standing);
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        RuleRegistry rules = new RuleRegistry();
        rules.load();

        return new RcaService(sagas, eventRepo, anomalies, results, workflows, new ObjectMapper(),
                new RcaSelector(), rules, new DetectionProperties(), alert -> { },
                new ExecutionProjectionService(), new FailureSignalRegistry(""), null)
                .analyze("saga-1");
    }

    private static Anomaly anomaly(Long id, AnomalyType type, Instant detectedAt) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setSagaId("saga-1");
        anomaly.setAnomalyType(type);
        anomaly.setDescription(type.name());
        anomaly.setDetectedAt(detectedAt);
        return anomaly;
    }
}
