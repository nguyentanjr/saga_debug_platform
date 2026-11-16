package com.example.correlation.service;

import com.example.correlation.domain.AnomalyType;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UnprovenOrderWithdrawalTest {
    private final AnomalyRepository anomalies = mock(AnomalyRepository.class);
    private final AnomalyDetectionService service = new AnomalyDetectionService(
            mock(SagaInstanceRepository.class), mock(SagaEventRepository.class), anomalies,
            mock(WorkflowRegistry.class), new ObjectMapper(), new DetectionProperties(),
            new ExecutionProjectionService());

    @Test
    void aStandingFindingIsWithdrawnSayingWhy() {
        when(anomalies.existsBySagaIdAndAnomalyType("run", AnomalyType.FORBIDDEN_POST_TERMINAL)).thenReturn(true);

        service.withdrawUnprovenOrder("run", AnomalyType.FORBIDDEN_POST_TERMINAL);

        verify(anomalies).retractClaims(eq("run"), eq(Set.of(AnomalyType.FORBIDDEN_POST_TERMINAL)), any(),
                contains("causal order"));
    }

    @Test
    void nothingStandingMeansNothingWritten() {
        when(anomalies.existsBySagaIdAndAnomalyType(anyString(), any())).thenReturn(false);

        service.withdrawUnprovenOrder("run", AnomalyType.STATE_REGRESSION);

        verify(anomalies, never()).retractClaims(anyString(), any(), any(), anyString());
    }
}
