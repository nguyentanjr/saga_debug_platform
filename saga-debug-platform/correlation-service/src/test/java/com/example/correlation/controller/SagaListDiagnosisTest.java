package com.example.correlation.controller;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.dto.SagaListItem;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SagaListDiagnosisTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void aRunShowsItsPromotedCauseAndTheStepThatCauseIsAnchoredTo() {
        SagaListItem item = listOne(saga(), List.of(), rca("R11", "FORWARD_STEP_FAILURE", T0,
                candidate("R11", "FORWARD_STEP_FAILURE", CandidateStatus.STRONG_EVIDENCE, "RESERVE_INVENTORY"),
                candidate("R02", "OTHER", CandidateStatus.RULED_OUT, "CREATE_ORDER")));

        assertThat(item.getRcaStatus()).isEqualTo("EVIDENCE_AVAILABLE");
        assertThat(item.getRootCause()).isEqualTo("FORWARD_STEP_FAILURE");
        assertThat(item.getRootCauseStep()).isEqualTo("RESERVE_INVENTORY");
    }

    @Test
    void theStepIsNeverTakenFromACandidateThatWasNotPromoted() {
        SagaListItem item = listOne(saga(), List.of(), rca("R11", "FORWARD_STEP_FAILURE", T0,
                candidate("R30", "FORWARD_STEP_FAILURE", CandidateStatus.SUPPORTING_EVIDENCE, "PROCESS_PAYMENT"),
                candidate("R11", "FORWARD_STEP_FAILURE", CandidateStatus.STRONG_EVIDENCE, null)));

        assertThat(item.getRootCause()).isEqualTo("FORWARD_STEP_FAILURE");
        assertThat(item.getRootCauseStep()).isNull();
    }

    @Test
    void aReanalysedRunShowsItsNewestDiagnosis() {
        RcaResult older = rca("R01", "CIRCUIT_BREAKER_OPEN", T0,
                candidate("R01", "CIRCUIT_BREAKER_OPEN", CandidateStatus.STRONG_EVIDENCE, "PROCESS_PAYMENT"));
        RcaResult newer = rca("R11", "FORWARD_STEP_FAILURE", T0.plusSeconds(60),
                candidate("R11", "FORWARD_STEP_FAILURE", CandidateStatus.STRONG_EVIDENCE, "RESERVE_INVENTORY"));

        SagaListItem item = listOne(saga(), List.of(), newer, older);

        assertThat(item.getRootCauseStep()).isEqualTo("RESERVE_INVENTORY");
    }

    @Test
    void onlyStandingAnomaliesAreCounted() {
        Anomaly retracted = anomaly();
        retracted.setRetractedAt(T0.plusSeconds(5));

        SagaListItem item = listOne(saga(), List.of(anomaly(), anomaly(), retracted));

        assertThat(item.getAnomalyCount()).isEqualTo(2);
        assertThat(item.getRcaStatus()).isNull();
        assertThat(item.getRootCause()).isNull();
    }

    @Test
    void aRunWithConflictingWorkflowsShowsNoCause() {
        SagaInstance saga = saga();
        saga.recordWorkflowConflict("LOAN_APPROVAL");

        SagaListItem item = listOne(saga, List.of(), rca("R11", "FORWARD_STEP_FAILURE", T0,
                candidate("R11", "FORWARD_STEP_FAILURE", CandidateStatus.STRONG_EVIDENCE, "RESERVE_INVENTORY")));

        assertThat(item.getRootCause()).isNull();
        assertThat(item.getRootCauseStep()).isNull();
    }

    private static SagaListItem listOne(SagaInstance saga, List<Anomaly> anomalies, RcaResult... results) {
        SagaInstanceRepository instances = mock(SagaInstanceRepository.class);
        when(instances.findAll(any(Pageable.class))).thenReturn(new PageImpl<>(List.of(saga)));
        SagaEventRepository events = mock(SagaEventRepository.class);
        when(events.countBySagaIdIn(anyList())).thenReturn(List.of());
        AnomalyRepository anomalyRepo = mock(AnomalyRepository.class);
        when(anomalyRepo.findBySagaIdIn(anyCollection())).thenReturn(anomalies);
        RcaResultRepository rcaRepo = mock(RcaResultRepository.class);
        when(rcaRepo.findBySagaIdInOrderByCreatedAtDesc(anyCollection())).thenReturn(List.of(results));

        return new SagaController(instances, events, anomalyRepo, rcaRepo, mock(ExecutionAliasRepository.class),
                mock(ExecutionLookup.class), new WorkflowRegistry())
                .listSagas(null, null, null, 0, 20).getBody().getContent().get(0);
    }

    private static SagaInstance saga() {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("default::run-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.COMPENSATED);
        saga.setStartTime(T0);
        saga.setLastUpdated(T0);
        return saga;
    }

    private static RcaResult rca(String ruleId, String rootCause, Instant at, RcaCandidate... candidates) {
        RcaResult result = new RcaResult();
        result.setSagaId("default::run-1");
        result.setRuleId(ruleId);
        result.setRootCause(rootCause);
        result.setRcaStatus(RcaStatus.EVIDENCE_AVAILABLE);
        result.setCandidates(List.of(candidates));
        result.setCreatedAt(at);
        return result;
    }

    private static RcaCandidate candidate(String ruleId, String rootCause, CandidateStatus status, String step) {
        RcaCandidate candidate = new RcaCandidate();
        candidate.setRuleId(ruleId);
        candidate.setRootCause(rootCause);
        candidate.setStatus(status);
        candidate.setAnchorStepId(step);
        return candidate;
    }

    private static Anomaly anomaly() {
        Anomaly anomaly = new Anomaly();
        anomaly.setSagaId("default::run-1");
        anomaly.setAnomalyType(AnomalyType.RETRY_BURST);
        return anomaly;
    }
}
