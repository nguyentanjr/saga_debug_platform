package com.example.correlation.controller;

import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.dto.SagaDetailResponse;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SagaDetailLifecycleTest {
    private static final Instant T0 = Instant.parse("2026-10-01T10:00:00Z");

    @Test
    void stateAtStartAndChangesDuringTheRunInTimeOrder() {
        ServiceLifecycleEventRepository repository = mock(ServiceLifecycleEventRepository.class);
        when(repository.findByServiceNameInAndOccurredAtBetweenOrderByOccurredAtAsc(anyCollection(), any(), any()))
                .thenReturn(List.of(
                        event("payment-service", ServiceLifecycleEvent.State.UP, T0.minusSeconds(3000)),
                        event("inventory-service", ServiceLifecycleEvent.State.DOWN, T0.minusSeconds(1800)),
                        event("inventory-service", ServiceLifecycleEvent.State.UP, T0.minusSeconds(1700)),
                        event("inventory-service", ServiceLifecycleEvent.State.DOWN, T0.minusSeconds(3)),
                        event("inventory-service", ServiceLifecycleEvent.State.UP, T0.plusSeconds(48))));

        List<SagaDetailResponse.LifecycleEntry> lifecycle = controller(repository).lifecycleFor(saga(), workflow());

        assertThat(lifecycle).extracting(e -> e.service() + ":" + e.state() + ":" + e.occurredAt())
                .containsExactly(
                        "payment-service:UP:" + T0.minusSeconds(3000),
                        "inventory-service:DOWN:" + T0.minusSeconds(3),
                        "inventory-service:UP:" + T0.plusSeconds(48));
    }

    @Test
    void onlyTheServicesTheWorkflowNamesAreAskedFor() {
        ServiceLifecycleEventRepository repository = mock(ServiceLifecycleEventRepository.class);
        @SuppressWarnings("unchecked")
        Collection<String>[] asked = new Collection[1];
        when(repository.findByServiceNameInAndOccurredAtBetweenOrderByOccurredAtAsc(anyCollection(), any(), any()))
                .thenAnswer(invocation -> {
                    asked[0] = invocation.getArgument(0);
                    return List.of();
                });

        controller(repository).lifecycleFor(saga(), workflow());

        assertThat(asked[0]).containsExactlyInAnyOrder("order-service", "payment-service", "inventory-service");
    }

    @Test
    void withoutALifecycleSourceTheListIsEmpty() {
        assertThat(controller(null).lifecycleFor(saga(), workflow())).isEmpty();
    }

    private static SagaController controller(ServiceLifecycleEventRepository repository) {
        SagaController controller = new SagaController(mock(SagaInstanceRepository.class),
                mock(SagaEventRepository.class), mock(AnomalyRepository.class), mock(RcaResultRepository.class),
                mock(ExecutionAliasRepository.class), mock(ExecutionLookup.class), new WorkflowRegistry());
        if (repository != null) controller.setLifecycleRepository(repository);
        return controller;
    }

    private static com.example.correlation.workflow.WorkflowSpec workflow() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        return registry.byType("ORDER_SAGA").orElseThrow();
    }

    private static SagaInstance saga() {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("default::run-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setStartTime(T0);
        saga.setEndTime(T0.plusSeconds(60));
        saga.setLastUpdated(T0);
        return saga;
    }

    private static ServiceLifecycleEvent event(String service, ServiceLifecycleEvent.State state, Instant at) {
        return new ServiceLifecycleEvent(service, state, at, "docker",
                state == ServiceLifecycleEvent.State.DOWN ? "die exitCode=1" : "start", service + at);
    }
}
