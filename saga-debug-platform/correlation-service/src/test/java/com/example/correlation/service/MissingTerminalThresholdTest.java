package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * F23: a run's MISSING_TERMINAL_EVENT threshold comes from its own workflow.
 *
 * <p>It was the slowest step across every loaded workflow, so a second system declaring a
 * 120s step moved ORDER_SAGA (slowest step 10s) from 100s to 1200s. Here ORDER_SAGA sits
 * beside such a workflow and is still held to 10s x 10.
 */
class MissingTerminalThresholdTest {

    private final WorkflowSpec orderSaga = WorkflowSpecTestFixture.orderSaga();
    private final WorkflowSpec slowWorkflow = WorkflowSpecTestFixture.load("workflows/order-saga-workflow.yaml");

    @Test
    void aRunIsHeldToItsOwnWorkflowNotTheSlowestLoaded() {
        assertThat(raised(Duration.ofSeconds(150))).containsExactly(AnomalyType.MISSING_TERMINAL_EVENT);
    }

    @Test
    void aRunInsideItsOwnThresholdIsNotReported() {
        assertThat(raised(Duration.ofSeconds(90))).isEmpty();
    }

    /** The finding is never rewritten, so its count must say when it was taken (1793611073159: 18 of 40). */
    @Test
    void theEventCountIsStatedAsOfDetection() {
        raised(Duration.ofSeconds(150));
        assertThat(descriptions).singleElement().asString()
                .startsWith("Saga had 2 events at ").contains("(when detected)");
    }

    private final List<String> descriptions = new ArrayList<>();

    private List<AnomalyType> raised(Duration silence) {
        slowWorkflow.setName("SLOW_PARTNER");
        slowWorkflow.getSteps().forEach(step -> step.setTimeout("120s"));

        SagaInstance saga = new SagaInstance();
        saga.setSagaId("default::run-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga.setLastUpdated(Instant.now().minus(silence));

        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        when(sagas.findByStatusInAndLastUpdatedBefore(any(), any(), any())).thenAnswer(c ->
                saga.getLastUpdated().isBefore(c.getArgument(1)) ? List.of(saga) : List.of());

        SagaEventRepository events = mock(SagaEventRepository.class);
        when(events.countBySagaId(anyString())).thenReturn(2L);
        SagaEvent last = new SagaEvent();
        last.setEventId("e2");
        last.setTimestamp(saga.getLastUpdated());
        when(events.findTopBySagaIdOrderByTimestampDesc(anyString())).thenReturn(Optional.of(last));

        List<AnomalyType> saved = new ArrayList<>();
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.save(any(Anomaly.class))).thenAnswer(c -> {
            saved.add(((Anomaly) c.getArgument(0)).getAnomalyType());
            descriptions.add(((Anomaly) c.getArgument(0)).getDescription());
            return c.getArgument(0);
        });

        WorkflowRegistry registry = mock(WorkflowRegistry.class);
        when(registry.workflowNames()).thenReturn(List.of("ORDER_SAGA", "SLOW_PARTNER"));
        when(registry.byType("ORDER_SAGA")).thenReturn(Optional.of(orderSaga));
        when(registry.byType("SLOW_PARTNER")).thenReturn(Optional.of(slowWorkflow));

        new AnomalyDetectionService(sagas, events, anomalies, registry, new ObjectMapper(),
                new DetectionProperties(), new ExecutionProjectionService())
                .detectMissingTerminalEvents();
        return saved;
    }
}
