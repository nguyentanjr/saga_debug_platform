package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TraceContextDetectionTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

    @Test
    void anEventThatLostTheTraceItsSiblingsCarryIsReported() {
        List<SagaEvent> events = List.of(
                traced("e1", "ORDER_CREATED", T0),
                untraced("e2", "PAYMENT_COMPLETED", T0.plusSeconds(1)),
                traced("e3", "INVENTORY_RESERVED", T0.plusSeconds(2)));

        Anomaly anomaly = detect(events.get(1), events);

        assertThat(anomaly).isNotNull();
        assertThat(anomaly.getAnomalyType()).isEqualTo(AnomalyType.MISSING_TRACE_CONTEXT);
        assertThat(anomaly.getEvidenceEventIds()).contains("e2");
    }

    @Test
    void aSourceThatPublishesNoTraceContextIsNotReportedAsBroken() {
        List<SagaEvent> events = List.of(
                untraced("e1", "ORDER_CREATED", T0),
                untraced("e2", "PAYMENT_COMPLETED", T0.plusSeconds(1)));

        assertThat(detect(events.get(1), events)).isNull();
    }

    @Test
    void aNestedTraceIdIsNotMistakenForABrokenPropagation() {
        SagaEvent ours = traced("e1", "ORDER_CREATED", T0);
        ours.setPayload("{\"traceId\":\"" + TRACE + "\"}");

        SagaEvent theirs = traced("e2", "PAYMENT_COMPLETED", T0.plusSeconds(1));
        theirs.setPayload("{\"context\":{\"traceId\":\"" + TRACE + "\"}}");

        SagaEvent alsoTheirs = traced("e3", "INVENTORY_RESERVED", T0.plusSeconds(2));
        alsoTheirs.setPayload("{\"context\":{\"traceId\":\"" + TRACE + "\"}}");

        List<SagaEvent> events = List.of(ours, theirs, alsoTheirs);

        assertThat(detect(theirs, events)).isNull();
    }

    private Anomaly detect(SagaEvent event, List<SagaEvent> sagaEvents) {
        List<Anomaly> saved = new ArrayList<>();
        AnomalyRepository repository = mock(AnomalyRepository.class);
        when(repository.save(any(Anomaly.class))).thenAnswer(invocation -> {
            saved.add(invocation.getArgument(0));
            return invocation.getArgument(0);
        });

        service(repository).detectMissingTraceContext("saga-1", sagaEvents);

        assertThat(saved).hasSizeLessThanOrEqualTo(1);
        return saved.isEmpty() ? null : saved.get(0);
    }

    private AnomalyDetectionService service(AnomalyRepository anomalyRepository) {
        return new AnomalyDetectionService(
                mock(SagaInstanceRepository.class),
                mock(SagaEventRepository.class),
                anomalyRepository,
                mock(WorkflowRegistry.class),
                new ObjectMapper(),
                new DetectionProperties(),
                new ExecutionProjectionService());
    }

    private static SagaEvent traced(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = untraced(eventId, eventType, timestamp);
        event.setTraceId(TRACE);
        return event;
    }

    private static SagaEvent untraced(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId("saga-1");
        event.setEventType(eventType);
        event.setServiceName("test");
        event.setTimestamp(timestamp);
        event.setPayload("{}");
        event.setReceivedAt(timestamp);
        return event;
    }
}
