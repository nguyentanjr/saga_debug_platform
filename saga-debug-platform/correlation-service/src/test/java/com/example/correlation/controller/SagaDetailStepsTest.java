package com.example.correlation.controller;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SagaDetailStepsTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void eachEventCarriesTheStepItsWorkflowPlacesItIn() {
        SagaInstance saga = saga();

        var detail = detail(saga, event(1L, "ORDER_CREATED"), event(2L, "PAYMENT_COMPLETED"),
                event(3L, "AUDIT_PING"));

        assertThat(detail.getWorkflowSteps())
                .isEqualTo(Map.of("1", "CREATE_ORDER", "2", "PROCESS_PAYMENT"));
        assertThat(detail.getEvents()).extracting(SagaEvent::getStepName).containsOnly("UNKNOWN");
    }

    @Test
    void aRunWithConflictingWorkflowsGetsNoPlacement() {
        SagaInstance saga = saga();
        saga.recordWorkflowConflict("LOAN_APPROVAL");

        var detail = detail(saga, event(1L, "ORDER_CREATED"));

        assertThat(detail.getWorkflowSteps()).isEmpty();
    }

    private static com.example.correlation.dto.SagaDetailResponse detail(SagaInstance saga, SagaEvent... events) {
        SagaInstanceRepository instances = mock(SagaInstanceRepository.class);
        when(instances.findById(saga.getSagaId())).thenReturn(Optional.of(saga));
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        when(eventRepo.findBySagaIdOrderByTimestampAsc(saga.getSagaId())).thenReturn(List.of(events));
        ExecutionLookup lookup = mock(ExecutionLookup.class);
        when(lookup.resolve(anyString())).thenReturn(Optional.of(saga.getSagaId()));
        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();

        return new SagaController(instances, eventRepo, mock(AnomalyRepository.class),
                mock(RcaResultRepository.class), mock(ExecutionAliasRepository.class), lookup, workflows)
                .getSagaDetail(saga.getSagaId()).getBody();
    }

    private static SagaInstance saga() {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("default::run-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga.setStartTime(T0);
        saga.setLastUpdated(T0);
        return saga;
    }

    private static SagaEvent event(Long id, String type) {
        SagaEvent event = new SagaEvent();
        event.setId(id);
        event.setSagaId("default::run-1");
        event.setEventId("e-" + id);
        event.setEventType(type);
        event.setStepName("UNKNOWN");
        event.setTimestamp(T0.plusSeconds(id));
        event.setPayload("{}");
        return event;
    }
}
