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

class EwalletCorePaymentDeclarationTest {
    private static final ExecutionKey RUN = ExecutionKey.of("ewallet-core-payment", "P-1");
    private static final String REPLIES = "PMT.EVENT-BACKBONE.REPLY-WALLET3-V2-STAGING";

    @Test
    void aReplyIsTheCommandAndTheStatusTogether() throws Exception {
        assertThat(replyType("{\"data\":{\"cmd\":\"XAC_NHAN_THANH_TOAN\",\"response\":{\"status\":\"SUCCESS_DEDUCT\"}}}"))
                .contains("HOLD_SUCCEEDED");
        assertThat(replyType("{\"data\":{\"cmd\":\"XAC_NHAN_THANH_TOAN_B2\",\"response\":{\"status\":\"SUCCESS_DEDUCT\"}}}"))
                .contains("SETTLE_SUCCEEDED");
        assertThat(replyType("{\"data\":{\"cmd\":\"XAC_NHAN_THANH_TOAN\",\"response\":{\"status\":\"FAILED_DEDUCT\"}}}"))
                .contains("HOLD_FAILED");
        assertThat(replyType("{\"data\":{\"cmd\":\"YEU_CAU_GACH_NO\",\"response\":{\"status\":\"TIMEOUT\"}}}"))
                .contains("PARTNER_PENDING");
    }

    @Test
    void theAuthorizeStatusIsReadFromInsideItsArray() throws Exception {
        assertThat(replyType("{\"data\":{\"cmd\":\"YEU_CAU_THANH_TOAN_USSD\",\"response\":{\"paymentSources\":[{\"status\":\"SUCCESS\"}]}}}"))
                .contains("AUTHORIZE_SUCCEEDED");
    }

    @Test
    void aSuccessfulPaymentWalksEveryStepAndEndsOnItsResult() {
        ReplayPipeline p = deliver(0, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PARTNER_STARTED", "PARTNER_SUCCEEDED", "SETTLE_STARTED", "SETTLE_SUCCEEDED", "PAYMENT_SUCCEEDED");

        assertThat(status(p)).isEqualTo(SagaStatus.COMPLETED);
        assertThat(standing(p)).isEmpty();
    }

    @Test
    void anUndeclaredThirdPartyApiIsToldApartFromAPartnerTimeout() throws Exception {
        assertThat(replyType("{\"data\":{\"cmd\":\"YEU_CAU_GACH_NO\",\"response\":{\"status\":\"TIMEOUT\",\"errorCode\":\"TP0023\"}}}"))
                .contains("PARTNER_NOT_CONFIGURED");
        assertThat(replyType("{\"data\":{\"cmd\":\"YEU_CAU_GACH_NO\",\"response\":{\"status\":\"TIMEOUT\",\"errorCode\":\"TP9999\"}}}"))
                .contains("PARTNER_PENDING");
        assertThat(replyType("{\"data\":{\"cmd\":\"YEU_CAU_GACH_NO\",\"response\":{\"status\":\"SUCCESS\",\"errorCode\":\"0000\"}}}"))
                .contains("PARTNER_SUCCEEDED");
    }

    @Test
    void endlessInquiriesDoNotKeepARunLookingAliveAndCannotWithdrawTheFinding() {
        ReplayPipeline p = deliver(16 * 60, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PARTNER_STARTED");
        send(p, Instant.now().minusSeconds(30), p.events.size(), "PARTNER_NOT_CONFIGURED");

        p.detector.detectStuckSagas();
        assertThat(standing(p)).contains(AnomalyType.SAGA_STUCK);
        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK).singleElement()
                .satisfies(a -> assertThat(a.getDescription()).contains("PARTNER_NOT_CONFIGURED"));

        deliverMore(p, "PARTNER_PENDING");
        assertThat(standing(p)).contains(AnomalyType.SAGA_STUCK);
    }

    @Test
    void aOneStepPaymentReachesItsResultWithoutAPartnerStep() {
        ReplayPipeline p = deliver(0, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PAYMENT_SUCCEEDED");

        assertThat(status(p)).isEqualTo(SagaStatus.COMPLETED);
        assertThat(standing(p)).isEmpty();
    }

    @Test
    void aResultAloneIsARun() {
        assertThat(status(deliver(0, "PAYMENT_FAILED"))).isEqualTo(SagaStatus.FAILED);
    }

    @Test
    void aRefusedHoldStaysOpenUntilTheResultEndsIt() {
        ReplayPipeline p = deliver(0, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_FAILED");
        assertThat(status(p).isTerminal()).isFalse();

        deliverMore(p, "PAYMENT_FAILED");

        assertThat(status(p)).isEqualTo(SagaStatus.FAILED);
        assertThat(standing(p)).doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    @Test
    void aPartnerTimeoutIsPatientForItsDeclaredWindowOnly() {
        ReplayPipeline recent = deliver(120, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PARTNER_STARTED", "PARTNER_PENDING");
        recent.detector.detectStuckSagas();
        assertThat(standing(recent)).doesNotContain(AnomalyType.SAGA_STUCK);

        ReplayPipeline old = deliver(16 * 60, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PARTNER_STARTED", "PARTNER_PENDING");
        old.detector.detectStuckSagas();
        assertThat(standing(old)).contains(AnomalyType.SAGA_STUCK);
    }

    private static final String RESULTS = "CORE-PAYMENT-TRANSACTION-RESULT";

    @Test
    void aResultIsReadWithItsSubStatus() throws Exception {
        assertThat(resultType("FAILED", "Payment_Service_Failed", "null")).contains("PAYMENT_RESULT_PENDING");
        assertThat(resultType("FAILED", "Revert_Money_Failed", null)).contains("PAYMENT_RESULT_PENDING");
        assertThat(resultType("SUCCESS", "Revenue_Money_OD_Failed (Step 2)", "null")).contains("PAYMENT_RESULT_PENDING");
        assertThat(resultType("FAILED", "Wait_Audit_Manual (Revert_Money_Failed)", "null")).contains("PAYMENT_WAIT_AUDIT");
        assertThat(resultType("WAIT_AUDIT", "Payment_Service_Timeout(3rd)", null)).contains("PAYMENT_WAIT_AUDIT");
        assertThat(resultType("FAILED", "Revert_Money_Success", "null")).contains("PAYMENT_FAILED");
        assertThat(resultType("SUCCESS", "Success", null)).contains("PAYMENT_SUCCEEDED");
        assertThat(resultType("SUCCESS", null, null)).contains("PAYMENT_SUCCEEDED");
        assertThat(resultType("FAILED", "REC_Collect_Success", "null")).contains("PAYMENT_FAILED");
    }

    @Test
    void aReconciliationTransactionUnderTheSameRequestIdIsLeftUnmapped() throws Exception {
        assertThat(resultType("SUCCESS", "REC_Collect_Success", "6263399800211511"))
                .contains("SUCCESS:REC_Collect_Success:6263399800211511");
        assertThat(type("CORE-UTILITIES-TRANSACTION-RESULT",
                "{\"status\":\"SUCCESS\",\"originalTransactionId\":\"6263399800211511\"}"))
                .contains("SUCCESS:6263399800211511");
        assertThat(type("CORE-UTILITIES-TRANSACTION-RESULT",
                "{\"status\":\"SUCCESS\",\"originalTransactionId\":\"null\"}")).contains("TRANSACTION_SUCCEEDED");
        assertThat(type("CORE-UTILITIES-TRANSACTION-RESULT", "{\"status\":\"WAIT_AUDIT\"}"))
                .contains("TRANSACTION_WAIT_AUDIT");
    }

    @Test
    void aResultRepublishedAsItsRevertMovesIsNoActivityAfterTheEnding() throws Exception {
        ReplayPipeline p = deliver(0, "HOLD_STARTED", "HOLD_SUCCEEDED", "PARTNER_STARTED", "PARTNER_FAILED");
        sendOn(p, RESULTS, resultType("FAILED", "Payment_Service_Failed", null).orElseThrow(),
                resultType("FAILED", "Revert_Money_Failed", null).orElseThrow(),
                resultType("FAILED", "Wait_Audit_Manual (Revert_Money_Failed)", null).orElseThrow());

        assertThat(status(p).isTerminal()).isFalse();
        assertThat(standing(p)).doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL, AnomalyType.MULTI_TERMINAL_CONFLICT);
    }

    @Test
    void aRevertThatSucceedsEndsTheRunOnce() throws Exception {
        ReplayPipeline p = deliver(0, "HOLD_STARTED", "HOLD_SUCCEEDED", "PARTNER_STARTED", "PARTNER_FAILED");
        sendOn(p, RESULTS, resultType("FAILED", "Payment_Service_Failed", "null").orElseThrow(),
                resultType("FAILED", "Revert_Money_Success", "null").orElseThrow());

        assertThat(status(p)).isEqualTo(SagaStatus.FAILED);
        assertThat(standing(p)).isEmpty();
    }

    @Test
    void aReconciliationTransactionIsNoSecondOutcome() throws Exception {
        ReplayPipeline p = deliver(0);
        sendOn(p, RESULTS, resultType("FAILED", "REC_Collect_Success", "null").orElseThrow(),
                resultType("SUCCESS", "REC_Collect_Success", "6263399800211511").orElseThrow());

        assertThat(status(p)).isEqualTo(SagaStatus.FAILED);
        assertThat(standing(p)).isEmpty();
    }

    @Test
    void anAuditResultDoesNotRestartTheSilenceClock() {
        ReplayPipeline p = deliver(16 * 60, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PARTNER_STARTED");
        send(p, Instant.now().minusSeconds(30), p.events.size(), "PAYMENT_WAIT_AUDIT");

        p.detector.detectStuckSagas();

        assertThat(status(p)).isEqualTo(SagaStatus.IN_PROGRESS);
        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK).hasSize(1);
    }

    @Test
    void aStuckFindingSaysHowManyRepeatsItIgnored() {
        ReplayPipeline p = deliver(16 * 60, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PARTNER_STARTED");
        send(p, Instant.now().minusSeconds(10 * 60), p.events.size(), "PARTNER_PENDING", "PARTNER_PENDING", "PARTNER_PENDING");

        p.detector.detectStuckSagas();

        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK).singleElement()
                .satisfies(a -> assertThat(a.getDescription())
                        .startsWith("Saga stuck: no progress since ")
                        .contains(", 3 repeat(s) without progress ignored"));
    }

    @Test
    void aStuckFindingWithNoRepeatsDoesNotMentionThem() {
        ReplayPipeline p = deliver(16 * 60, "AUTHORIZE_STARTED", "AUTHORIZE_SUCCEEDED", "HOLD_STARTED", "HOLD_SUCCEEDED",
                "PARTNER_STARTED");

        p.detector.detectStuckSagas();

        assertThat(p.anomalies).filteredOn(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK).singleElement()
                .satisfies(a -> assertThat(a.getDescription()).doesNotContain("repeat"));
    }

    private static java.util.Optional<String> resultType(String status, String subStatus, String original) throws Exception {
        com.fasterxml.jackson.databind.node.ObjectNode json = new ObjectMapper().createObjectNode().put("status", status);
        if (subStatus != null) json.put("transactionSubStatus", subStatus);
        if (original != null) json.put("originalTransactionId", original);
        return type(RESULTS, json.toString());
    }

    private static void sendOn(ReplayPipeline p, String topic, String... types) {
        int offset = p.events.size();
        Instant from = Instant.now().plusSeconds(1);
        for (int i = 0; i < types.length; i++) {
            Instant at = from.plusMillis(i * 100L);
            p.deliver(List.of(new IngestedEvent(
                    new IngestedEvent.Identity("ewallet-core-payment", RUN, "e" + (offset + i), null, CorrelationBasis.DECLARED_KEY),
                    "EWALLET_CORE_PAYMENT", types[i], null, "svc", at, "{}", at, IngestedEvent.Causality.NONE,
                    topic + "-0-" + (offset + i))));
        }
    }

    private static java.util.Optional<String> replyType(String json) throws Exception {
        return type(REPLIES, json);
    }

    private static java.util.Optional<String> type(String topic, String json) throws Exception {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", declarations().toString());
        registry.load();
        SourceMapping mapping = registry.forTopic(topic);
        return mapping.eventType(new ObjectMapper().readTree(json), null);
    }

    private static ReplayPipeline deliver(int secondsAgo, String... types) {
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", declarations().toString());
        workflows.load();
        ReplayPipeline p = new ReplayPipeline(workflows);
        send(p, Instant.now().minusSeconds(secondsAgo), 0, types);
        return p;
    }

    private static void deliverMore(ReplayPipeline p, String... types) {
        send(p, Instant.now().plusSeconds(1), p.events.size(), types);
    }

    private static void send(ReplayPipeline p, Instant from, int offset, String... types) {
        for (int i = 0; i < types.length; i++) {
            Instant at = from.plusMillis(i * 100L);
            p.deliver(List.of(new IngestedEvent(
                    new IngestedEvent.Identity("ewallet-core-payment", RUN, "e" + (offset + i), null, CorrelationBasis.DECLARED_KEY),
                    "EWALLET_CORE_PAYMENT", types[i], null, "svc", at, "{}", at, IngestedEvent.Causality.NONE,
                    REPLIES + "-0-" + (offset + i))));
        }
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
