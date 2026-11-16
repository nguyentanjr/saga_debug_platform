package com.example.correlation.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

class CompensationAckDeclarationTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SAGA_ID = "kms::ORD-1";

    private static final String ACKNOWLEDGING_WORKFLOW = """
            workflow:
              name: ACK_SAGA
              version: "1.0"
              steps:
                - id: RESERVE_STOCK
                  events:
                    started: STOCK_REQUESTED
                    completed: STOCK_RESERVED
                    failed: STOCK_FAILED
              compensationSteps:
                - id: ROLLBACK_STOCK
                  events:
                    started: STOCK_ROLLBACK_REQUESTED
                    completed: STOCK_ROLLBACK_DONE
              transitions:
                - from: STARTED
                  to: RESERVE_STOCK
            """;

    private static final String SILENT_WORKFLOW = """
            workflow:
              name: SILENT_SAGA
              version: "1.0"
              steps:
                - id: RESERVE_STOCK
                  events:
                    started: STOCK_REQUESTED
                    completed: STOCK_RESERVED
                    failed: STOCK_FAILED
              compensationSteps:
                - id: ROLLBACK_STOCK
                  events:
                    started: STOCK_ROLLBACK_REQUESTED
              transitions:
                - from: STARTED
                  to: RESERVE_STOCK
            """;

    @Test
    @DisplayName("a rollback that declared an acknowledgement and never gave one is still reported")
    void anOwedAcknowledgementIsStillAFinding() {
        List<Anomaly> raised = run(ACKNOWLEDGING_WORKFLOW, compensationStarted());

        assertThat(raised).singleElement()
                .satisfies(anomaly -> {
                    assertThat(anomaly.getAnomalyType()).isEqualTo(AnomalyType.MISSING_COMPENSATION_ACK);
                    assertThat(anomaly.getEvidenceEventIds()).contains("e2");
                });
    }

    @Test
    @DisplayName("the same silence on a system that never declared an acknowledgement is not a finding")
    void aRollbackThatPromisedNothingIsNotMissingAnything() {
        assertThat(run(SILENT_WORKFLOW, compensationStarted())).isEmpty();
    }

    @Test
    @DisplayName("an acknowledgement that did arrive closes the question")
    void anAcknowledgedRollbackIsNotReported() {
        List<SagaEvent> events = new ArrayList<>(compensationStarted());
        events.add(event("e3", "STOCK_ROLLBACK_DONE", T0.plusSeconds(20)));

        assertThat(run(ACKNOWLEDGING_WORKFLOW, events)).isEmpty();
    }

    @Test
    @DisplayName("a workflow declaring the completion as an outcome counts as declaring one")
    void completionDeclaredAsAnOutcomeCounts() {
        String outcomeWorkflow = """
                workflow:
                  name: OUTCOME_SAGA
                  version: "1.0"
                  steps:
                    - id: RESERVE_STOCK
                      events:
                        started: STOCK_REQUESTED
                        completed: STOCK_RESERVED
                  compensationSteps:
                    - id: ROLLBACK_STOCK
                      events:
                        started: STOCK_ROLLBACK_REQUESTED
                      outcomes:
                        REVERSED: STOCK_PUT_BACK
                  transitions:
                    - from: STARTED
                      to: RESERVE_STOCK
                """;

        assertThat(spec(outcomeWorkflow).declaresCompletionEvent("ROLLBACK_STOCK")).isTrue();
        assertThat(run(outcomeWorkflow, compensationStarted())).hasSize(1);
    }

    @Test
    @DisplayName("the question is asked of the step that started, not of the workflow as a whole")
    void theQuestionIsPerStep() {
        String mixedWorkflow = """
                workflow:
                  name: MIXED_SAGA
                  version: "1.0"
                  steps:
                    - id: RESERVE_STOCK
                      events:
                        started: STOCK_REQUESTED
                        completed: STOCK_RESERVED
                  compensationSteps:
                    - id: ROLLBACK_STOCK
                      events:
                        started: STOCK_ROLLBACK_REQUESTED
                    - id: REFUND_PAYMENT
                      events:
                        started: REFUND_REQUESTED
                        completed: REFUND_DONE
                  transitions:
                    - from: STARTED
                      to: RESERVE_STOCK
                """;
        WorkflowSpec spec = spec(mixedWorkflow);

        assertThat(spec.declaresCompletionEvent("ROLLBACK_STOCK")).isFalse();
        assertThat(spec.declaresCompletionEvent("REFUND_PAYMENT")).isTrue();

        assertThat(run(mixedWorkflow, compensationStarted())).isEmpty();

        List<SagaEvent> refundStarted = List.of(
                event("e1", "STOCK_FAILED", T0),
                event("e2", "STOCK_ROLLBACK_REQUESTED", T0.plusSeconds(5)),
                event("e3", "REFUND_REQUESTED", T0.plusSeconds(6)));
        List<Anomaly> raised = run(mixedWorkflow, refundStarted);
        assertThat(raised).singleElement().satisfies(anomaly -> {
            assertThat(anomaly.getObservedValue()).contains("REFUND_REQUESTED");
            assertThat(anomaly.getObservedValue()).doesNotContain("STOCK_ROLLBACK_REQUESTED");
        });
    }

    @Test
    @DisplayName("an unknown step declares nothing, so it is owed nothing")
    void anUnknownStepDeclaresNothing() {
        assertThat(spec(SILENT_WORKFLOW).declaresCompletionEvent("NO_SUCH_STEP")).isFalse();
        assertThat(spec(SILENT_WORKFLOW).declaresCompletionEvent(null)).isFalse();
    }

    private static List<SagaEvent> compensationStarted() {
        return List.of(
                event("e1", "STOCK_FAILED", T0),
                event("e2", "STOCK_ROLLBACK_REQUESTED", T0.plusSeconds(5)));
    }

    private List<Anomaly> run(String workflowYaml, List<SagaEvent> events) {
        WorkflowSpec spec = spec(workflowYaml);

        SagaInstance saga = new SagaInstance();
        saga.setSagaId(SAGA_ID);
        saga.setWorkflowType(spec.getName());
        saga.setStatus(SagaStatus.COMPENSATING);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findByStatusInAndLastUpdatedBefore(any(), any(), any()))
                .thenReturn(List.of(saga));

        SagaEventRepository sagaEvents = mock(SagaEventRepository.class);
        when(sagaEvents.findBySagaIdOrderByTimestampAsc(SAGA_ID)).thenReturn(events);

        List<Anomaly> raised = new ArrayList<>();
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.existsBySagaIdAndAnomalyType(anyString(), any())).thenReturn(false);
        lenient().when(anomalies.save(any(Anomaly.class))).thenAnswer(call -> {
            raised.add(call.getArgument(0));
            return call.getArgument(0);
        });

        WorkflowRegistry registry = mock(WorkflowRegistry.class);
        lenient().when(registry.byType(anyString())).thenReturn(Optional.of(spec));
        lenient().when(registry.workflowNames()).thenReturn(List.of(spec.getName()));

        new AnomalyDetectionService(sagaInstances, sagaEvents, anomalies, registry,
                new ObjectMapper(), new DetectionProperties(), new ExecutionProjectionService())
                .detectMissingCompensationAck();

        return raised;
    }

    @SuppressWarnings("unchecked")
    private static WorkflowSpec spec(String yaml) {
        try {
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> root = yamlMapper.readValue(yaml, Map.class);
            return yamlMapper.convertValue(root.get("workflow"), WorkflowSpec.class);
        } catch (Exception e) {
            throw new IllegalStateException("bad test workflow", e);
        }
    }

    private static SagaEvent event(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId(SAGA_ID);
        event.setEventType(eventType);
        event.setServiceName("test");
        event.setTimestamp(timestamp);
        event.setReceivedAt(timestamp);
        event.setPayload("{}");
        return event;
    }
}
