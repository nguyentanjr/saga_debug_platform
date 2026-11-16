package com.example.correlation.service.rca;

import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionOrderRankingTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final RcaSelector selector = new RcaSelector();
    private final WorkflowSpec spec = WorkflowSpecTestFixture.orderSaga();

    private final RcaSelector.ExecutionOrder order = new RcaSelector.ExecutionOrder() {
        private final List<String> steps =
                List.of("CREATE_ORDER", "PROCESS_PAYMENT", "RESERVE_INVENTORY", "REFUND_PAYMENT");

        @Override
        public OptionalInt positionOf(String stepId) {
            int index = steps.indexOf(stepId);
            return index < 0 ? OptionalInt.empty() : OptionalInt.of(index);
        }

        @Override
        public boolean isUpstream(String candidate, String step) {
            return spec.isUpstreamOf(candidate, step);
        }
    };

    @Test
    void theEarlierDivergenceIsRankedAboveTheLaterOne() {
        CandidateAssessment lateButAlphabeticallyFirst = anchored(
                "R07", "DOWNSTREAM_TIMEOUT", "HIGH", "RESERVE_INVENTORY", T0.plusSeconds(30));
        CandidateAssessment earlier = anchored(
                "R10", "COMPENSATION_STEP_FAILURE", "HIGH", "PROCESS_PAYMENT", T0.plusSeconds(5));

        RcaSelector.SelectionResult result = selector.select(
                List.of(lateButAlphabeticallyFirst, earlier), List.of(),
                saga(SagaStatus.COMPENSATING), order);

        assertThat(ruleIds(result)).containsExactly("R10", "R07");
    }

    @Test
    void evidenceTierStillOutranksExecutionPosition() {
        CandidateAssessment earlyButWeak = anchored(
                "R21", "STUCK_UNKNOWN_CAUSE", "HIGH", "CREATE_ORDER", T0);
        earlyButWeak.setStatus(CandidateStatus.WEAK_EVIDENCE);
        CandidateAssessment lateButStrong = anchored(
                "R07", "DOWNSTREAM_TIMEOUT", "HIGH", "RESERVE_INVENTORY", T0.plusSeconds(30));

        RcaSelector.SelectionResult result = selector.select(
                List.of(earlyButWeak, lateButStrong), List.of(), saga(SagaStatus.FAILED), order);

        assertThat(ruleIds(result)).containsExactly("R07", "R21");
    }

    @Test
    void aDownstreamCandidateRecordsWhatPrecededIt() {
        CandidateAssessment upstream = anchored(
                "R02", "TIMEOUT_RETRY_STORM", "CRITICAL", "PROCESS_PAYMENT", T0.plusSeconds(5));
        CandidateAssessment downstream = anchored(
                "R07", "DOWNSTREAM_TIMEOUT", "HIGH", "RESERVE_INVENTORY", T0.plusSeconds(30));

        RcaSelector.SelectionResult result = selector.select(
                List.of(upstream, downstream), List.of(), saga(SagaStatus.FAILED), order);

        assertThat(find(result, "R07").getPrecededBy()).containsExactly("R02");
        assertThat(find(result, "R07").getStatusReason())
                .contains("downstream of")
                .contains("no causal link established");

        assertThat(find(result, "R02").getPrecededBy()).isEmpty();
    }

    @Test
    void stepsOnUnrelatedPathsAreNotCalledDownstreamOfEachOther() {
        CandidateAssessment compensation = anchored(
                "R10", "COMPENSATION_STEP_FAILURE", "HIGH", "REFUND_PAYMENT", T0.plusSeconds(40));
        CandidateAssessment inventory = anchored(
                "R07", "DOWNSTREAM_TIMEOUT", "HIGH", "RESERVE_INVENTORY", T0.plusSeconds(30));

        RcaSelector.SelectionResult result = selector.select(
                List.of(compensation, inventory), List.of(), saga(SagaStatus.COMPENSATING), order);

        assertThat(find(result, "R10").getPrecededBy()).isEmpty();
    }

    @Test
    void anUnanchoredCandidateSortsLastWithoutBeingGivenAPosition() {
        CandidateAssessment anchored = anchored(
                "R07", "DOWNSTREAM_TIMEOUT", "HIGH", "RESERVE_INVENTORY", T0.plusSeconds(30));
        CandidateAssessment floating = candidate("R03", "MULTI_TERMINAL_CONFLICT", "CRITICAL");

        RcaSelector.SelectionResult result = selector.select(
                List.of(floating, anchored), List.of(), saga(SagaStatus.FAILED), order);

        assertThat(ruleIds(result)).containsExactly("R07", "R03");
        assertThat(find(result, "R03").getAnchorStepId()).isNull();
    }

    @Test
    void orderingIsDeterministicWhenNothingDistinguishesTwoCandidates() {
        CandidateAssessment a = candidate("R12", "INVALID_TRANSITION", "MEDIUM");
        CandidateAssessment b = candidate("R03", "MULTI_TERMINAL_CONFLICT", "MEDIUM");

        assertThat(ruleIds(selector.select(List.of(a, b), List.of(), saga(SagaStatus.FAILED), order)))
                .isEqualTo(ruleIds(selector.select(List.of(b, a), List.of(), saga(SagaStatus.FAILED), order)));
    }

    private static List<String> ruleIds(RcaSelector.SelectionResult result) {
        return result.getCandidates().stream().map(RcaCandidate::getRuleId).toList();
    }

    private static RcaCandidate find(RcaSelector.SelectionResult result, String ruleId) {
        return result.getCandidates().stream()
                .filter(c -> ruleId.equals(c.getRuleId()))
                .findFirst().orElseThrow();
    }

    private static CandidateAssessment candidate(String ruleId, String rootCause, String severity) {
        return CandidateAssessment.builder()
                .ruleId(ruleId)
                .rootCause(rootCause)
                .severity(severity)
                .status(CandidateStatus.STRONG_EVIDENCE)
                .build();
    }

    private static CandidateAssessment anchored(String ruleId, String rootCause, String severity,
            String stepId, Instant at) {
        CandidateAssessment assessment = candidate(ruleId, rootCause, severity);
        assessment.setAnchorStepId(stepId);
        assessment.setAnchorAt(at);
        return assessment;
    }

    private static SagaInstance saga(SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setStatus(status);
        return saga;
    }
}
