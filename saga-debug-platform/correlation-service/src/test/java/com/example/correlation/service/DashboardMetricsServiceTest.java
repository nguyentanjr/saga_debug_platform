package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.dto.MetricItem;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DashboardMetricsServiceTest {
    private static final Instant T0 = Instant.now().minusSeconds(600);

    @Test
    void aFindingIsCountedAgainstTheStepOfWhatItCitesNeverTheWorkflowOrAnEventName() {
        List<Anomaly> anomalies = List.of(

                anomaly(AnomalyType.INVALID_TRANSITION, "workflow:ORDER_SAGA.transitions",
                        "Invalid transition: CREATE_ORDER -> PROCESS_PAYMENT", "e2"),

                anomaly(AnomalyType.SAGA_STUCK, "config:anomaly.detection.default-step-timeout-seconds",
                        "Saga stuck: nothing heard since ... (last: INVENTORY_RESERVED)", "e3"),

                anomaly(AnomalyType.STEP_TIMEOUT, "workflow:PROCESS_PAYMENT.timeout",
                        "Step 'PROCESS_PAYMENT' took 12000ms", "e1", "e2"),

                anomaly(AnomalyType.OUT_OF_ORDER, null,
                        "Event 'PAYMENT_FAILED' arrived after 'INVENTORY_RESERVED'", "e2", "e3"));

        List<MetricItem> steps = summarize(anomalies).topAffectedSteps();

        assertThat(steps).extracting(MetricItem::name)
                .containsExactlyInAnyOrder("PROCESS_PAYMENT", "RESERVE_INVENTORY");
        assertThat(steps).filteredOn(m -> m.name().equals("PROCESS_PAYMENT"))
                .singleElement().extracting(MetricItem::count).isEqualTo(2L);
    }

    @Test
    void aWithdrawnFindingIsNotCountedAsAnAnomaly() {
        Anomaly standing = anomaly(AnomalyType.STEP_TIMEOUT, "workflow:PROCESS_PAYMENT.timeout",
                "Step 'PROCESS_PAYMENT' took 12000ms", "e1", "e2");
        Anomaly withdrawn = anomaly(AnomalyType.ORPHAN_EVENT, null, "no start event yet", "e2");
        withdrawn.setRetractedAt(T0.plusSeconds(90));
        withdrawn.setRetractionReason("the start event arrived");

        var summary = summarize(List.of(standing, withdrawn));

        assertThat(summary.totalAnomalies()).isEqualTo(1);
        assertThat(summary.topAnomalyTypes()).extracting(MetricItem::name).containsExactly("STEP_TIMEOUT");
        assertThat(summary.topAffectedSteps()).extracting(MetricItem::count).containsExactly(1L);
    }

    private com.example.correlation.dto.DashboardSummaryResponse summarize(List<Anomaly> anomalies) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("default::run-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.FAILED);
        saga.setStartTime(T0);
        saga.setLastUpdated(T0);

        SagaInstanceRepository instances = mock(SagaInstanceRepository.class);
        when(instances.findAllById(any())).thenReturn(List.of(saga));
        AnomalyRepository anomalyRepository = mock(AnomalyRepository.class);
        when(anomalyRepository.findByDetectedAtAfter(any())).thenReturn(anomalies);
        RcaResultRepository results = mock(RcaResultRepository.class);
        SagaEventRepository events = mock(SagaEventRepository.class);
        when(events.findBySagaIdInOrderBySagaIdAscTimestampAsc(anyList())).thenReturn(List.of(
                event("e1", "ORDER_CREATED", 0),
                event("e2", "PAYMENT_FAILED", 12),
                event("e3", "INVENTORY_RESERVED", 13)));
        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();

        return new DashboardMetricsService(instances, anomalyRepository, results, events, workflows)
                .summarize(24);
    }

    private static Anomaly anomaly(AnomalyType type, String thresholdSource, String description,
            String... eventIds) {
        Anomaly anomaly = new Anomaly();
        anomaly.setSagaId("default::run-1");
        anomaly.setAnomalyType(type);
        anomaly.setThresholdSource(thresholdSource);
        anomaly.setDescription(description);
        anomaly.setObservedValue(description);
        anomaly.setEvidenceEventIds(List.of(eventIds));
        anomaly.setDetectedAt(T0.plusSeconds(60));
        return anomaly;
    }

    private static SagaEvent event(String id, String type, int second) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("default::run-1");
        event.setSourceName("default");
        event.setEventId(id);
        event.setEventType(type);
        event.setPayload("{}");
        event.setTimestamp(T0.plusSeconds(second));
        return event;
    }
}
