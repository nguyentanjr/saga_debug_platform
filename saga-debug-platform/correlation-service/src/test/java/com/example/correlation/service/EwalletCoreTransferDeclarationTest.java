package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.ingestion.SourceMapping;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The transfer workflows against what staging produced on 2026-11-02: a cash out from bank that
 * expired and was unheld, and a transfer to bank that succeeded.
 */
class EwalletCoreTransferDeclarationTest {

    private static final ExecutionKey RUN = ExecutionKey.of("ewallet-core-transfer", "A1");

    @Test
    void messagesReadAsTheirWorkflowNamesThem() throws Exception {
        assertThat(type("staging-ewallet-transfer-management-v2", "{\"eventType\":\"TRQ\",\"transferType\":\"WTW\",\"subTransferType\":1}")).contains("W2N_REQUESTED");
        assertThat(type("staging-ewallet-transfer-management-w2o", "{\"eventType\":\"TRQ\",\"transferType\":\"WTO\"}")).contains("W2O_REQUESTED");
        assertThat(type("staging-ewallet-transfer-management-v2", "{\"eventType\":\"UH\"}")).contains("UNHOLD_STARTED");
        assertThat(type("staging-ewallet-transfer-management-v2", "{\"eventType\":\"TRQ\",\"transferType\":\"WTW\",\"subTransferType\":2}")).contains("W2N_SECRET_CODE_ENTERED");
        assertThat(type("staging-ewallet-transfer-management-v2", "{\"eventType\":\"UH\",\"transactionType\":\"REFUND_EXPIRY_WITHDRAWAL\"}")).contains("W2N_EXPIRED");
        assertThat(type("staging-ewallet-transfer-management-v2", "{\"eventType\":\"RM\",\"transactionType\":\"REFUND_EXPIRY_WITHDRAWAL\"}")).contains("W2N_EXPIRED");
        assertThat(type("staging-ewallet-transfer-business-v2", "{\"eventType\":\"TRP\",\"transactionSubStatus\":\"W2N_Holding_Success\"}")).contains("W2N_HOLD_SUCCEEDED");
        assertThat(type("staging-ewallet-transfer-business-v2", "{\"eventType\":\"UH\",\"transactionStatus\":\"SUC\"}")).contains("UNHOLD_SUCCEEDED");
        assertThat(type("CORE-TRANSFER-TRANSACTION-RESULT", "{\"transactionSubStatus\":\"W2N_Holding_Success\"}")).contains("W2N_Holding_Success");
        assertThat(type("CORE-TRANSFER-TRANSACTION-RESULT", "{\"transactionSubStatus\":\"W2O_Credit_O_Timeout (Partner)\"}")).contains("W2O_SEND_PENDING");
    }

    @Test
    void anExpiredCashOutIsCompensatedAndTheRepublishedHoldIsNoFinding() {
        ReplayPipeline p = new ReplayPipeline(workflows());
        Instant at = Instant.now().minusSeconds(400);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e1", "W2N_REQUESTED", 0);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e2", "W2N_HOLD_SUCCEEDED", 100);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e3", "W2N_EXPIRED", 358_000);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e4", "UNHOLD_SUCCEEDED", 358_100);
        send(p, "EWALLET_W2N_TRANSFER", at, "CORE-TRANSFER-TRANSACTION-RESULT", "e5", "W2N_Holding_Success", 358_120);
        send(p, "EWALLET_W2N_TRANSFER", at, "CORE-TRANSFER-TRANSACTION-RESULT", "e6", "UNHOLD_SUCCEEDED", 358_121);

        assertThat(status(p)).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(standing(p)).doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void aSecretCodeWithdrawalCompletes() {
        ReplayPipeline p = new ReplayPipeline(workflows());
        Instant at = Instant.now().minusSeconds(120);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e1", "W2N_REQUESTED", 0);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e2", "W2N_HOLD_SUCCEEDED", 100);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e3", "W2N_SECRET_CODE_ENTERED", 60_000);
        assertThat(status(p)).isEqualTo(SagaStatus.IN_PROGRESS);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e4", "W2N_RELEASE_SUCCEEDED", 60_100);

        assertThat(status(p)).isEqualTo(SagaStatus.COMPLETED);
        assertThat(standing(p)).isEmpty();
    }

    @Test
    void aHoldThatNeitherExpiresNorIsWithdrawnIsReportedOnTheWaitingStep() {
        ReplayPipeline p = new ReplayPipeline(workflows());
        Instant at = Instant.now().minusSeconds(11 * 60);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e1", "W2N_REQUESTED", 0);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e2", "W2N_HOLD_SUCCEEDED", 100);

        p.detector.detectStuckSagas();

        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK).singleElement()
                .satisfies(a -> assertThat(a.getThresholdSource()).isEqualTo("workflow:AWAIT_WITHDRAWAL.timeout"));
    }

    @Test
    void aReleaseTimeoutReadsAsPendingOnEveryChannel() throws Exception {
        assertThat(type("staging-ewallet-transfer-business-v2", "{\"transactionSubStatus\":\"W2N_Release_Timeout\"}")).contains("W2N_RELEASE_PENDING");
        assertThat(type("CORE-TRANSFER-TRANSACTION-RESULT", "{\"transactionSubStatus\":\"W2N_Release_Timeout\"}")).contains("W2N_RELEASE_PENDING");
    }

    @Test
    void aReleaseTheTimeoutJobNeverResolvesIsReportedOnTheReleaseStep() {
        ReplayPipeline p = new ReplayPipeline(workflows());
        Instant at = Instant.now().minusSeconds(16 * 60);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e1", "W2N_REQUESTED", 0);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e2", "W2N_HOLD_SUCCEEDED", 100);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e3", "W2N_SECRET_CODE_ENTERED", 5_000);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e4", "W2N_RELEASE_PENDING", 10_000);

        p.detector.detectStuckSagas();

        assertThat(status(p)).isEqualTo(SagaStatus.IN_PROGRESS);
        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK).singleElement()
                .satisfies(a -> assertThat(a.getThresholdSource()).isEqualTo("workflow:RELEASE_MONEY.timeout"));
    }

    @Test
    void aReleaseTheTimeoutJobResolvesInTimeCompletes() {
        ReplayPipeline p = new ReplayPipeline(workflows());
        Instant at = Instant.now().minusSeconds(16 * 60);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e1", "W2N_REQUESTED", 0);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e2", "W2N_HOLD_SUCCEEDED", 100);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-management-v2", "e3", "W2N_SECRET_CODE_ENTERED", 5_000);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e4", "W2N_RELEASE_PENDING", 10_000);
        send(p, "EWALLET_W2N_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e5", "W2N_RELEASE_SUCCEEDED", 300_000);

        p.detector.detectStuckSagas();

        assertThat(status(p)).isEqualTo(SagaStatus.COMPLETED);
        assertThat(standing(p)).isEmpty();
    }

    @Test
    void aSuccessfulTransferToBankCompletes() {
        ReplayPipeline p = new ReplayPipeline(workflows());
        Instant at = Instant.now().minusSeconds(60);
        send(p, "EWALLET_W2O_TRANSFER", at, "staging-ewallet-transfer-management-w2o", "e1", "W2O_REQUESTED", 0);
        send(p, "EWALLET_W2O_TRANSFER", at, "staging-ewallet-transfer-business-v2", "e2", "W2O_SEND_SUCCEEDED", 5_600);
        send(p, "EWALLET_W2O_TRANSFER", at, "CORE-TRANSFER-TRANSACTION-RESULT", "e3", "W2O_SEND_SUCCEEDED", 5_620);

        assertThat(status(p)).isEqualTo(SagaStatus.COMPLETED);
        assertThat(standing(p)).isEmpty();
    }

    @Test
    void eachRequestStartsItsOwnWorkflow() {
        WorkflowRegistry workflows = workflows();
        assertThat(workflows.forStartEvent("W2N_REQUESTED", "ewallet-core-transfer").orElseThrow().getName()).isEqualTo("EWALLET_W2N_TRANSFER");
        assertThat(workflows.forStartEvent("W2O_REQUESTED", "ewallet-core-transfer").orElseThrow().getName()).isEqualTo("EWALLET_W2O_TRANSFER");
    }

    private static java.util.Optional<String> type(String topic, String json) throws Exception {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", declarations().toString());
        registry.load();
        SourceMapping mapping = registry.forTopic(topic);
        return mapping.eventType(new ObjectMapper().readTree(json), null);
    }

    private static WorkflowRegistry workflows() {
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", declarations().toString());
        workflows.load();
        return workflows;
    }

    private static void send(ReplayPipeline p, String workflow, Instant from, String topic, String id, String type, long afterMs) {
        Instant at = from.plusMillis(afterMs);
        p.deliver(List.of(new IngestedEvent(
                new IngestedEvent.Identity("ewallet-core-transfer", RUN, id, null, CorrelationBasis.DECLARED_KEY),
                workflow, type, null, "svc", at, "{}", at, IngestedEvent.Causality.NONE, topic + "-0-" + id.substring(1))));
    }

    private static SagaStatus status(ReplayPipeline p) {
        return p.instances.values().iterator().next().getStatus();
    }

    private static List<AnomalyType> standing(ReplayPipeline p) {
        return p.anomalies.stream().filter(a -> a.getRetractedAt() == null).map(Anomaly::getAnomalyType).toList();
    }

    private static Path declarations() {
        Path here = Path.of("").toAbsolutePath();
        return (Files.exists(here.resolve("declarations")) ? here : here.getParent()).resolve("declarations");
    }
}
