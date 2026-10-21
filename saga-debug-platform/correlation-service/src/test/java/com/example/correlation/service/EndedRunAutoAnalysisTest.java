package com.example.correlation.service;

import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * L17: ended runs are analysed without anyone calling /rca/analyze. Which runs qualify is
 * SagaIdsAwaitingAnalysisTest; this pins what the sweep does with them.
 */
class EndedRunAutoAnalysisTest {

    private final SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
    private final RcaService self = mock(RcaService.class);
    private final RcaService sweep = new RcaService(sagas, mock(SagaEventRepository.class),
            mock(AnomalyRepository.class), mock(RcaResultRepository.class), new WorkflowRegistry(),
            new ObjectMapper(), new RcaSelector(), new RuleRegistry(), new DetectionProperties(),
            alert -> { }, new ExecutionProjectionService(), new FailureSignalRegistry(""), self);

    @Test
    @SuppressWarnings("unchecked")
    void everyRunAwaitingAnalysisIsAnalysedInItsOwnTransaction() {
        when(sagas.findSagaIdsAwaitingAnalysis(any(), any())).thenReturn(List.of("a", "b"));

        sweep.autoAnalyzeEndedRuns();

        verify(self).analyze("a");
        verify(self).analyze("b");
        ArgumentCaptor<Collection<SagaStatus>> statuses = ArgumentCaptor.forClass(Collection.class);
        verify(sagas).findSagaIdsAwaitingAnalysis(statuses.capture(), any());
        assertThat(statuses.getValue()).containsExactlyInAnyOrder(SagaStatus.FAILED, SagaStatus.COMPENSATED);
    }

    /**
     * A run diagnosed while it waited and then completed is analysed again once evidence
     * outdates the diagnosis, even though completing raised no new finding.
     */
    @Test
    @SuppressWarnings("unchecked")
    void completedRunsWithFindingsAndNewerEvidenceAreAnalysedAgain() {
        when(sagas.findSagaIdsWithFindingsAwaitingAnalysis(any(), any())).thenReturn(List.of("recovered"));

        sweep.autoAnalyzeEndedRuns();

        verify(self).analyze("recovered");
        ArgumentCaptor<Collection<SagaStatus>> statuses = ArgumentCaptor.forClass(Collection.class);
        verify(sagas).findSagaIdsWithFindingsAwaitingAnalysis(statuses.capture(), any());
        assertThat(statuses.getValue()).containsExactly(SagaStatus.COMPLETED);
    }

    @Test
    void oneRunThatCannotBeAnalysedDoesNotStopTheRest() {
        when(sagas.findSagaIdsAwaitingAnalysis(any(), any())).thenReturn(List.of("broken", "fine"));
        when(self.analyze("broken")).thenThrow(new IllegalStateException("boom"));

        sweep.autoAnalyzeEndedRuns();

        verify(self).analyze("fine");
    }
}
