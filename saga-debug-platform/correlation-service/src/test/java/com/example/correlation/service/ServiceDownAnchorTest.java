package com.example.correlation.service;

import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.service.rca.ServiceDownRule;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * R25's anchor survives the analysis pass that anchors candidates at their earliest cited
 * event (EXT-R25-MISS-KITCHEN, live FTGO batches 7-9). R25 cites the event that closed the
 * wait; when that event belongs to a later step — a source without per-step completions —
 * the generic anchoring moved a crash of the awaited step's service to the next step, while
 * the evidence still said which step was awaited: every FTGO crash came out one step late.
 */
class ServiceDownAnchorTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void aCrashStaysAnchoredAtTheStepTheRunWasWaitingOn() {
        SagaEvent created = event("e1", "ORDER_CREATED", 0);
        SagaEvent inventoryStarted = event("e2", "INVENTORY_STARTED", 50);  // closes the wait, later step

        RcaResult result = analyze(created, inventoryStarted);

        assertThat(result.getCandidates()).filteredOn(c -> "R25".equals(c.getRuleId()))
                .singleElement()
                .extracting(RcaCandidate::getAnchorStepId)
                .isEqualTo("PROCESS_PAYMENT");
    }

    private static RcaResult analyze(SagaEvent... events) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setStartTime(events[0].getTimestamp());
        saga.setEndTime(events[events.length - 1].getTimestamp());

        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        when(sagas.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(List.of(events));
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.findStandingBySagaId(anyString())).thenReturn(List.of());
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        ServiceLifecycleEventRepository lifecycle = mock(ServiceLifecycleEventRepository.class);
        when(lifecycle.findByServiceNameInAndOccurredAtBetweenOrderByOccurredAtAsc(any(), any(), any()))
                .thenReturn(List.of(
                        new ServiceLifecycleEvent("payment-service", ServiceLifecycleEvent.State.DOWN,
                                T0.minusSeconds(1), "docker", "die exitCode=143", "p:die"),
                        new ServiceLifecycleEvent("payment-service", ServiceLifecycleEvent.State.UP,
                                T0.plusSeconds(45), "docker", "start", "p:start")));

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        RuleRegistry rules = new RuleRegistry();
        rules.load();
        RcaService rca = new RcaService(sagas, eventRepo, anomalies, results, workflows, new ObjectMapper(),
                new RcaSelector(), rules, new DetectionProperties(), alert -> { },
                new ExecutionProjectionService(), new FailureSignalRegistry(""), null);
        rca.setServiceDownRule(new ServiceDownRule(lifecycle, Duration.ofHours(1)));
        return rca.analyze("saga-1");
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
