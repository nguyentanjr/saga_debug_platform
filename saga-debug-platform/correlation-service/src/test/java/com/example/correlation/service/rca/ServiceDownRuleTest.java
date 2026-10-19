package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R25: a crash is named only when the runtime reported the awaited step's service down
 * during a wait longer than the step allows. Shapes follow the evaluation scenarios that
 * stop inventory-service: SC04/SC16 (stopped, run waits, restarted, run completes) and
 * SC03/SC17 (stopped, run still waiting at analysis time).
 */
class ServiceDownRuleTest {

    private static final Instant T0 = Instant.parse("2026-10-01T10:00:00Z");
    private WorkflowSpec workflow;

    @BeforeEach
    void loadWorkflow() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        workflow = registry.byType("ORDER_SAGA").orElseThrow();
    }

    @Test
    void stoppedAndRestartedWhileTheRunWaitedIsACrashAnchoredAtTheAwaitedStep() {
        SagaInstance saga = saga(SagaStatus.COMPLETED);
        List<SagaEvent> events = recoveredRun();

        Optional<ServiceDownRule.Finding> finding = assess(saga, events, T0.plusSeconds(60),
                down("inventory-service", T0.minusSeconds(3)), up("inventory-service", T0.plusSeconds(48)));

        assertThat(finding).isPresent();
        CandidateAssessment c = finding.get().candidate();
        assertThat(c.getRootCause()).isEqualTo("SERVICE_CRASH");
        assertThat(c.getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
        assertThat(c.getAnchorStepId()).isEqualTo("RESERVE_INVENTORY");
        assertThat(c.getSeverity()).isEqualTo("MEDIUM");
        assertThat(c.getEvidence()).flatExtracting(Evidence::eventIds).containsExactly("e3");
        assertThat(c.getEvidence()).extracting(Evidence::fact)
                .anyMatch(f -> f.contains("inventory-service exited"))
                .anyMatch(f -> f.contains("started again"));
    }

    @Test
    void stillDownWhileTheRunIsStillWaitingIsACriticalCrash() {
        SagaInstance saga = saga(SagaStatus.IN_PROGRESS);
        List<SagaEvent> events = List.of(event("e1", "ORDER_CREATED", 0), event("e2", "PAYMENT_COMPLETED", 1));

        Optional<ServiceDownRule.Finding> finding = assess(saga, events, T0.plusSeconds(150),
                down("inventory-service", T0.minusSeconds(3)));

        assertThat(finding).isPresent();
        assertThat(finding.get().candidate().getSeverity()).isEqualTo("CRITICAL");
        assertThat(finding.get().candidate().getAnchorStepId()).isEqualTo("RESERVE_INVENTORY");
        assertThat(finding.get().explainedWait().closingEventId()).isNull();
    }

    /**
     * A wait belongs to the step awaited after the event that opened it, not to the step of
     * the event that closed it (EXT-R25-MISS-KITCHEN). A source that does not publish every
     * step's completion — an orchestrator seen only through its commands, or a completion
     * event no mapping translates — closes the wait with a later step's event: here payment
     * was down after ORDER_CREATED and the next thing heard is inventory starting. The old
     * reading blamed RESERVE_INVENTORY's service and, finding it up, claimed nothing — but
     * only once the run had ended; while it was still waiting the open-wait reading was
     * right, so the result depended on when RCA happened to run.
     */
    @Test
    void aWaitClosedByALaterStepsEventIsChargedToTheStepThatWasAwaited() {
        List<SagaEvent> run = List.of(event("e1", "ORDER_CREATED", 0), event("e2", "INVENTORY_STARTED", 50));

        Optional<ServiceDownRule.Finding> finding = assess(saga(SagaStatus.COMPLETED), run, T0.plusSeconds(60),
                down("payment-service", T0.minusSeconds(1)), up("payment-service", T0.plusSeconds(45)));

        assertThat(finding).isPresent();
        assertThat(finding.get().candidate().getAnchorStepId()).isEqualTo("PROCESS_PAYMENT");
        assertThat(finding.get().explainedWait().closingEventId()).isEqualTo("e2");
    }

    /** The same run, but the closing step's own service was the one restarted: not the cause. */
    @Test
    void theClosingStepsServiceDidNotHoldUpAWaitThatEndedWithItsOwnEvent() {
        List<SagaEvent> run = List.of(event("e1", "ORDER_CREATED", 0), event("e2", "INVENTORY_STARTED", 50));

        assertThat(assess(saga(SagaStatus.COMPLETED), run, T0.plusSeconds(60),
                down("inventory-service", T0.minusSeconds(1)), up("inventory-service", T0.plusSeconds(45))))
                .isEmpty();
    }

    /** No runtime report — the collector off, a pause, a SIGSTOP, a partition: R05/R06 as before. */
    @Test
    void withoutAnyRuntimeReportNothingIsClaimed() {
        assertThat(assess(saga(SagaStatus.COMPLETED), recoveredRun(), T0.plusSeconds(60))).isEmpty();
    }

    @Test
    void aDifferentServiceBeingDownExplainsNothing() {
        assertThat(assess(saga(SagaStatus.COMPLETED), recoveredRun(), T0.plusSeconds(60),
                down("payment-service", T0.plusSeconds(5)), up("payment-service", T0.plusSeconds(40))))
                .isEmpty();
    }

    @Test
    void aRestartThatFinishedBeforeTheWaitExplainsNothing() {
        assertThat(assess(saga(SagaStatus.COMPLETED), recoveredRun(), T0.plusSeconds(60),
                down("inventory-service", T0.minusSeconds(30)), up("inventory-service", T0.minusSeconds(20))))
                .isEmpty();
    }

    /** A restart while runs are in flight is not blamed on a run that never waited past its timeout. */
    @Test
    void aRunThatNeverWaitedPastTheTimeoutIsNotBlamed() {
        List<SagaEvent> fast = List.of(event("e1", "ORDER_CREATED", 0), event("e2", "PAYMENT_COMPLETED", 1),
                event("e3", "INVENTORY_RESERVED", 3));
        assertThat(assess(saga(SagaStatus.COMPLETED), fast, T0.plusSeconds(10),
                down("inventory-service", T0.plusSeconds(2)), up("inventory-service", T0.plusSeconds(4))))
                .isEmpty();
    }

    /** A start with no observed exit gives no interval: its beginning would be invented. */
    @Test
    void aStartWithoutAnObservedExitIsNotADownInterval() {
        assertThat(assess(saga(SagaStatus.COMPLETED), recoveredRun(), T0.plusSeconds(60),
                up("inventory-service", T0.plusSeconds(48)))).isEmpty();
    }

    @Test
    void theTimeoutAndSilenceOfThatWaitBecomeConsequencesAndTheCrashIsPromoted() {
        SagaInstance saga = saga(SagaStatus.COMPLETED);
        ServiceDownRule.Finding finding = assess(saga, recoveredRun(), T0.plusSeconds(60),
                down("inventory-service", T0.minusSeconds(3)), up("inventory-service", T0.plusSeconds(48)))
                .orElseThrow();

        Anomaly timeout = anomaly(1L, AnomalyType.STEP_TIMEOUT, "workflow:RESERVE_INVENTORY.timeout", T0.plusSeconds(50));
        Anomaly stuck = anomaly(2L, AnomalyType.SAGA_STUCK, "config", T0.plusSeconds(32));
        CandidateAssessment r07 = strong("R07", "DOWNSTREAM_TIMEOUT", "HIGH", Evidence.of("timeout", 1L));
        CandidateAssessment r06 = strong("R06", "RECOVERED_AFTER_SILENCE", "LOW", Evidence.of("silence", null));
        List<CandidateAssessment> candidates = new ArrayList<>(List.of(r07, r06, finding.candidate()));

        ServiceDownRule.explainSymptoms(candidates, List.of(timeout, stuck), finding);
        var selection = new RcaSelector().select(candidates, List.of(timeout, stuck), saga);

        assertThat(r07.getStatus()).isEqualTo(CandidateStatus.SUPPORTING_EVIDENCE);
        assertThat(r06.getStatus()).isEqualTo(CandidateStatus.SUPPORTING_EVIDENCE);
        assertThat(r07.getStatusReason()).contains("R25");
        assertThat(selection.getPrimary().getRuleId()).isEqualTo("R25");
    }

    /** Another step's timeout, or a silence outside the wait, is not this crash's consequence. */
    @Test
    void findingsAboutOtherWaitsAreLeftAsTheyWere() {
        ServiceDownRule.Finding finding = assess(saga(SagaStatus.COMPLETED), recoveredRun(), T0.plusSeconds(60),
                down("inventory-service", T0.minusSeconds(3)), up("inventory-service", T0.plusSeconds(48)))
                .orElseThrow();

        Anomaly paymentTimeout = anomaly(1L, AnomalyType.STEP_TIMEOUT, "workflow:PROCESS_PAYMENT.timeout", T0);
        Anomaly earlyStuck = anomaly(2L, AnomalyType.SAGA_STUCK, "config", T0.minusSeconds(100));
        CandidateAssessment r07 = strong("R07", "DOWNSTREAM_TIMEOUT", "HIGH", Evidence.of("timeout", 1L));
        CandidateAssessment r06 = strong("R06", "RECOVERED_AFTER_SILENCE", "LOW", Evidence.of("silence", null));

        ServiceDownRule.explainSymptoms(new ArrayList<>(List.of(r07, r06)), List.of(paymentTimeout, earlyStuck), finding);

        assertThat(r07.getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
        assertThat(r06.getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
    }

    /**
     * Two deployments both run a payment-service (the demo and the hotel saga, observed at
     * once). With no declared scope, a name two scopes report cannot be pinned on this run.
     */
    @Test
    void aServiceNameTwoDeploymentsReportIsNotAttributedWithoutADeclaredScope() {
        List<ServiceLifecycleEvent> lifecycle = List.of(
                down("inventory-service", T0.minusSeconds(3)).inScope("hotel-real"),
                up("inventory-service", T0.plusSeconds(48)).inScope("hotel-real"),
                down("inventory-service", T0.plusSeconds(100)).inScope("saga-debug-platform"));

        assertThat(ServiceDownRule.assess(saga(SagaStatus.COMPLETED),
                ServiceDownRule.waits(saga(SagaStatus.COMPLETED), recoveredRun(), workflow, T0.plusSeconds(60)),
                ServiceDownRule.readable(lifecycle, null))).isEmpty();
    }

    @Test
    void aDeclaredScopeReadsOnlyItsOwnDeployment() {
        List<ServiceLifecycleEvent> lifecycle = List.of(
                down("inventory-service", T0.minusSeconds(3)).inScope("hotel-real"),
                up("inventory-service", T0.plusSeconds(48)).inScope("hotel-real"),
                down("inventory-service", T0.minusSeconds(3)).inScope("saga-debug-platform"),
                up("inventory-service", T0.plusSeconds(48)).inScope("saga-debug-platform"));

        assertThat(ServiceDownRule.readable(lifecycle, "hotel-real")).hasSize(2)
                .allSatisfy(e -> assertThat(e.getScope()).isEqualTo("hotel-real"));
        assertThat(ServiceDownRule.readable(lifecycle, "kms")).isEmpty();
    }

    /** One deployment only: the name is unambiguous and is used, scoped or not. */
    @Test
    void aSingleScopeIsUsedWithoutADeclaration() {
        List<ServiceLifecycleEvent> lifecycle = List.of(
                down("inventory-service", T0.minusSeconds(3)).inScope("saga-debug-platform"),
                up("inventory-service", T0.plusSeconds(48)).inScope("saga-debug-platform"));
        assertThat(ServiceDownRule.readable(lifecycle, null)).hasSize(2);
    }

    /** A probe that could not reach the service is also what a partition looks like. */
    @Test
    void aFailedProbeCorroboratesACrashWithoutSettlingIt() {
        SagaInstance saga = saga(SagaStatus.COMPLETED);
        ServiceDownRule.Finding finding = assess(saga, recoveredRun(), T0.plusSeconds(60),
                probe("inventory-service", ServiceLifecycleEvent.State.DOWN, T0.minusSeconds(3)),
                probe("inventory-service", ServiceLifecycleEvent.State.UP, T0.plusSeconds(48)))
                .orElseThrow();

        assertThat(finding.candidate().getStatus()).isEqualTo(CandidateStatus.SUPPORTING_EVIDENCE);
        assertThat(finding.candidate().getStatusReason()).contains("cannot tell");

        Anomaly timeout = anomaly(1L, AnomalyType.STEP_TIMEOUT, "workflow:RESERVE_INVENTORY.timeout", T0.plusSeconds(50));
        CandidateAssessment r07 = strong("R07", "DOWNSTREAM_TIMEOUT", "HIGH", Evidence.of("timeout", 1L));
        ServiceDownRule.explainSymptoms(new ArrayList<>(List.of(r07, finding.candidate())), List.of(timeout), finding);
        assertThat(r07.getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
    }

    @Test
    void aProcessReportOutranksAProbeForTheSameWait() {
        Optional<ServiceDownRule.Finding> finding = assess(saga(SagaStatus.COMPLETED), recoveredRun(), T0.plusSeconds(60),
                probe("inventory-service", ServiceLifecycleEvent.State.DOWN, T0.minusSeconds(10)),
                probe("inventory-service", ServiceLifecycleEvent.State.UP, T0.plusSeconds(5)),
                down("inventory-service", T0.plusSeconds(10)), up("inventory-service", T0.plusSeconds(48)));

        assertThat(finding.orElseThrow().candidate().getStatus()).isEqualTo(CandidateStatus.STRONG_EVIDENCE);
    }

    private static ServiceLifecycleEvent probe(String service, ServiceLifecycleEvent.State state, Instant at) {
        return new ServiceLifecycleEvent(service, state, at, "alertmanager", "up==0",
                service + ":" + state + ":" + at, ServiceLifecycleEvent.Basis.PROBE);
    }

    private Optional<ServiceDownRule.Finding> assess(SagaInstance saga, List<SagaEvent> events, Instant now,
            ServiceLifecycleEvent... lifecycle) {
        return ServiceDownRule.assess(saga, ServiceDownRule.waits(saga, events, workflow, now), List.of(lifecycle));
    }

    /** Payment done at +1s, inventory's reply heard only at +50s (timeout 5s). */
    private static List<SagaEvent> recoveredRun() {
        return List.of(event("e1", "ORDER_CREATED", 0), event("e2", "PAYMENT_COMPLETED", 1),
                event("e3", "INVENTORY_RESERVED", 50));
    }

    private static SagaInstance saga(SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("default::run-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(status);
        saga.setStartTime(T0);
        saga.setLastUpdated(T0);
        return saga;
    }

    private static SagaEvent event(String id, String type, int atSecond) {
        SagaEvent e = new SagaEvent();
        e.setEventId(id);
        e.setEventType(type);
        e.setTimestamp(T0.plusSeconds(atSecond));
        e.setReceivedAt(T0.plusSeconds(atSecond));
        return e;
    }

    private static ServiceLifecycleEvent down(String service, Instant at) {
        return new ServiceLifecycleEvent(service, ServiceLifecycleEvent.State.DOWN, at, "docker",
                "die exitCode=143", service + ":die:" + at);
    }

    private static ServiceLifecycleEvent up(String service, Instant at) {
        return new ServiceLifecycleEvent(service, ServiceLifecycleEvent.State.UP, at, "docker",
                "start", service + ":start:" + at);
    }

    private static Anomaly anomaly(Long id, AnomalyType type, String source, Instant detectedAt) {
        Anomaly a = new Anomaly();
        a.setId(id);
        a.setSagaId("default::run-1");
        a.setAnomalyType(type);
        a.setThresholdSource(source);
        a.setDetectedAt(detectedAt);
        return a;
    }

    private static CandidateAssessment strong(String ruleId, String cause, String severity, Evidence evidence) {
        CandidateAssessment c = CandidateAssessment.builder().ruleId(ruleId).rootCause(cause)
                .severity(severity).status(CandidateStatus.STRONG_EVIDENCE).build();
        c.getEvidence().add(evidence);
        return c;
    }
}
