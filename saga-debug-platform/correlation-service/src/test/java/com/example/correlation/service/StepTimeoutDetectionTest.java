package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.EventTimeSource;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class StepTimeoutDetectionTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final WorkflowSpec spec = WorkflowSpecTestFixture.orderSaga();

    private final List<SagaEvent> slowPayment = List.of(
            event("e1", "ORDER_CREATED", T0),
            event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(12)),
            event("e3", "INVENTORY_RESERVED", T0.plusSeconds(13)));

    @Test
    void bothMeasurementsAgreeOnAPlainSlowStep() {
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.LEGACY, slowPayment))
                .isEqualTo("PROCESS_PAYMENT");
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.PROJECTION, slowPayment))
                .isEqualTo("PROCESS_PAYMENT");
    }

    @Test
    void unrelatedObservationHidesTheTimeoutFromTheLegacyMeasurement() {
        List<SagaEvent> withNoise = new ArrayList<>(slowPayment);
        withNoise.add(event("e-noise", "AUDIT_PING", T0.plusSeconds(11)));

        assertThat(findingStep(DetectionProperties.StepTimeoutMode.LEGACY, withNoise)).isNull();
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.PROJECTION, withNoise))
                .isEqualTo("PROCESS_PAYMENT");
    }

    @Test
    void retriesHideTheTimeoutFromTheLegacyMeasurement() {
        List<SagaEvent> retriedPayment = List.of(
                event("e1", "ORDER_CREATED", T0),
                event("e2", "PAYMENT_RETRY", T0.plusSeconds(3)),
                event("e3", "PAYMENT_RETRY", T0.plusSeconds(6)),
                event("e4", "PAYMENT_RETRY", T0.plusSeconds(9)),
                event("e5", "PAYMENT_FAILED", T0.plusSeconds(11)));

        assertThat(findingStep(DetectionProperties.StepTimeoutMode.LEGACY, retriedPayment)).isNull();

        Anomaly projected = capture(DetectionProperties.StepTimeoutMode.PROJECTION, retriedPayment);
        assertThat(projected).isNotNull();
        assertThat(projected.getObservedValue())
                .contains("PROCESS_PAYMENT took 11000ms")
                .contains("over 4 attempt(s)");
    }

    private final List<SagaEvent> lateButQuickRefund = List.of(
            event("e1", "ORDER_CREATED", T0),
            event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)),
            event("e3", "INVENTORY_FAILED", T0.plusSeconds(4)),
            event("e4", "REFUND_PAYMENT", T0.plusSeconds(39)),
            event("e5", "PAYMENT_REFUNDED", T0.plusSeconds(44)));

    @Test
    void theDelayBeforeAStepStartsIsNotThatStepsDuration() {
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.LEGACY, lateButQuickRefund))
                .isEqualTo("REFUND_PAYMENT");
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.PROJECTION, lateButQuickRefund))
                .isNull();
    }

    @Test
    void aSlowStepWithAnObservedStartIsStillReported() {
        List<SagaEvent> slowRefund = List.of(
                event("e1", "ORDER_CREATED", T0),
                event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)),
                event("e3", "INVENTORY_FAILED", T0.plusSeconds(4)),
                event("e4", "REFUND_PAYMENT", T0.plusSeconds(5)),
                event("e5", "PAYMENT_REFUNDED", T0.plusSeconds(17)));

        Anomaly anomaly = capture(DetectionProperties.StepTimeoutMode.PROJECTION, slowRefund);
        assertThat(anomaly).isNotNull();
        assertThat(anomaly.getObservedValue())
                .contains("REFUND_PAYMENT took 12000ms")
                .contains("start=OBSERVED_START");
    }

    @Test
    void theDefaultMeasurementIsTheProjection() {
        assertThat(new DetectionProperties().getStepTimeoutMode())
                .isEqualTo(DetectionProperties.StepTimeoutMode.PROJECTION);
    }

    @Test
    void shadowModeKeepsLegacyAuthoritative() {
        List<SagaEvent> withNoise = new ArrayList<>(slowPayment);
        withNoise.add(event("e-noise", "AUDIT_PING", T0.plusSeconds(11)));

        assertThat(findingStep(DetectionProperties.StepTimeoutMode.SHADOW, withNoise)).isNull();
    }

    @Test
    void projectionRecordsMeasurementProvenance() {
        Anomaly anomaly = capture(DetectionProperties.StepTimeoutMode.PROJECTION, slowPayment);

        assertThat(anomaly.getObservedValue()).contains("start=INFERRED_FROM_PREDECESSOR");
        assertThat(anomaly.getThresholdSource()).isEqualTo("workflow:PROCESS_PAYMENT.timeout");
        assertThat(anomaly.getEvidenceEventIds()).contains("e1", "e2");
    }

    @Test
    void projectionDoesNotInventADurationForAnUnanchoredOperation() {
        List<SagaEvent> orphan = List.of(
                event("e1", "INVENTORY_RESERVED", T0.plusSeconds(3600)));

        AnomalyRepository repository = mock(AnomalyRepository.class);
        service(DetectionProperties.StepTimeoutMode.PROJECTION, repository)
                .evaluateStepTimeouts("saga-1", orphan, spec);

        verifyNoInteractions(repository);
    }

    @Test
    void aDurationBetweenTwoDeclaredTimesSaysSo() {
        List<SagaEvent> declared = withSources(EventTimeSource.DECLARED, EventTimeSource.DECLARED);

        for (DetectionProperties.StepTimeoutMode mode : List.of(
                DetectionProperties.StepTimeoutMode.LEGACY, DetectionProperties.StepTimeoutMode.PROJECTION)) {
            assertThat(capture(mode, declared).getObservedValue()).as(mode.name())
                    .contains("clocks=DECLARED->DECLARED");
        }
    }

    @Test
    void aDurationAcrossADeclaredAndATransportTimeIsMarkedMixed() {
        List<SagaEvent> mixed = withSources(EventTimeSource.DECLARED, EventTimeSource.TRANSPORT);

        for (DetectionProperties.StepTimeoutMode mode : List.of(
                DetectionProperties.StepTimeoutMode.LEGACY, DetectionProperties.StepTimeoutMode.PROJECTION)) {
            Anomaly anomaly = capture(mode, mixed);
            assertThat(anomaly.getObservedValue()).as(mode.name())
                    .contains("clocks=DECLARED->TRANSPORT (mixed)");
            assertThat(anomaly.getDescription()).as(mode.name()).contains("different clocks");
        }
    }

    @Test
    void aDurationOverTimesOfUnrecordedOriginSaysSo() {
        List<SagaEvent> legacy = withSources(null, EventTimeSource.DECLARED);

        for (DetectionProperties.StepTimeoutMode mode : List.of(
                DetectionProperties.StepTimeoutMode.LEGACY, DetectionProperties.StepTimeoutMode.PROJECTION)) {
            assertThat(capture(mode, legacy).getObservedValue()).as(mode.name())
                    .contains("clocks=unrecorded");
        }
    }

    private List<SagaEvent> withSources(EventTimeSource start, EventTimeSource end) {
        SagaEvent created = event("e1", "ORDER_CREATED", T0);
        created.setEventTimeSource(start);
        SagaEvent paid = event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(12));
        paid.setEventTimeSource(end);
        SagaEvent reserved = event("e3", "INVENTORY_RESERVED", T0.plusSeconds(13));
        reserved.setEventTimeSource(EventTimeSource.DECLARED);
        return List.of(created, paid, reserved);
    }

    private String findingStep(DetectionProperties.StepTimeoutMode mode, List<SagaEvent> events) {
        Anomaly anomaly = capture(mode, events);
        if (anomaly == null) return null;

        String source = anomaly.getThresholdSource();
        return source.substring("workflow:".length(), source.indexOf(".timeout"));
    }

    private Anomaly capture(DetectionProperties.StepTimeoutMode mode, List<SagaEvent> events) {
        List<Anomaly> saved = new ArrayList<>();
        AnomalyRepository repository = mock(AnomalyRepository.class);
        when(repository.save(any(Anomaly.class))).thenAnswer(invocation -> {
            saved.add(invocation.getArgument(0));
            return invocation.getArgument(0);
        });

        List<SagaEvent> ordered = events.stream()
                .sorted(java.util.Comparator.comparing(SagaEvent::getTimestamp))
                .toList();

        service(mode, repository).evaluateStepTimeouts("saga-1", ordered, spec);

        assertThat(saved).hasSizeLessThanOrEqualTo(1);
        if (saved.isEmpty()) return null;

        Anomaly anomaly = saved.get(0);
        assertThat(anomaly.getAnomalyType()).isEqualTo(AnomalyType.STEP_TIMEOUT);
        return anomaly;
    }

    private AnomalyDetectionService service(DetectionProperties.StepTimeoutMode mode,
            AnomalyRepository anomalyRepository) {
        DetectionProperties properties = new DetectionProperties();
        properties.setStepTimeoutMode(mode);

        WorkflowRegistry registry = mock(WorkflowRegistry.class);
        org.mockito.Mockito.lenient().when(registry.byType(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(java.util.Optional.of(spec));
        org.mockito.Mockito.lenient().when(registry.workflowNames())
                .thenReturn(java.util.List.of(spec.getName()));

        return new AnomalyDetectionService(
                mock(SagaInstanceRepository.class),
                mock(SagaEventRepository.class),
                anomalyRepository,
                registry,
                new ObjectMapper(),
                properties,
                new ExecutionProjectionService());
    }

    private static SagaEvent event(String eventId, String eventType, Instant timestamp) {
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
