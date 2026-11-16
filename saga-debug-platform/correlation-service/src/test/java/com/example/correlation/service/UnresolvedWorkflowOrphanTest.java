package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UnresolvedWorkflowOrphanTest {
    private static final Instant T0 = Instant.parse("2026-10-11T23:39:00Z");

    private final List<Anomaly> raised = new ArrayList<>();
    private final AnomalyDetectionService detector = detector(
            workflow("ORDER_SAGA", "ORDER_CREATED"), workflow("EVENTUATE_CREATE_ORDER", "ORDER_CREATED"));

    @Test
    void anAmbiguousStartIsStillAStart() {
        detector.detectOrphanEvent(null, "run", List.of(event("ORDER_CREATED")));

        assertThat(raised).isEmpty();
    }

    @Test
    void aRunWithNoKnownStartIsStillReported() {
        detector.detectOrphanEvent(null, "run", List.of(event("CUSTOMER_CREDIT_RESERVED")));

        assertThat(raised).singleElement()
                .satisfies(a -> assertThat(a.getAnomalyType()).isEqualTo(AnomalyType.ORPHAN_EVENT));
    }

    private AnomalyDetectionService detector(WorkflowSpec... specs) {
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.existsBySagaIdAndAnomalyType(anyString(), any())).thenReturn(false);
        lenient().when(anomalies.save(any(Anomaly.class))).thenAnswer(call -> {
            raised.add(call.getArgument(0));
            return call.getArgument(0);
        });
        WorkflowRegistry registry = mock(WorkflowRegistry.class);
        when(registry.workflowNames()).thenReturn(List.of(specs).stream().map(WorkflowSpec::getName).toList());
        for (WorkflowSpec spec : specs) {
            when(registry.byType(spec.getName())).thenReturn(Optional.of(spec));
        }
        return new AnomalyDetectionService(mock(SagaInstanceRepository.class), mock(SagaEventRepository.class),
                anomalies, registry, new ObjectMapper(), new DetectionProperties(), new ExecutionProjectionService());
    }

    @SuppressWarnings("unchecked")
    private static WorkflowSpec workflow(String name, String start) {
        String yaml = """
                workflow:
                  name: %s
                  version: "1.0"
                  steps:
                    - id: CREATE
                      events:
                        started: %s
                        completed: %s
                  transitions:
                    - from: STARTED
                      to: CREATE
                """.formatted(name, start, start);
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> root = mapper.readValue(yaml, Map.class);
            return mapper.convertValue(root.get("workflow"), WorkflowSpec.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static SagaEvent event(String type) {
        SagaEvent event = new SagaEvent();
        event.setEventId("e1");
        event.setSagaId("run");
        event.setEventType(type);
        event.setServiceName("svc");
        event.setTimestamp(T0);
        event.setReceivedAt(T0);
        event.setPayload("{}");
        return event;
    }
}
