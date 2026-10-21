package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;

import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L17: which ended runs the auto-RCA sweep picks up. Actual query on an actual schema;
 * the l17.* properties run it on Postgres, as EventTimeProvenanceTest does with f6.*.
 */
@DataJpaTest(showSql = false, properties = {
        "spring.datasource.url=${l17.jdbc.url:jdbc:h2:mem:l17;MODE=PostgreSQL;DB_CLOSE_DELAY=-1}",
        "spring.datasource.driver-class-name=${l17.jdbc.driver:org.h2.Driver}",
        "spring.datasource.username=${l17.jdbc.username:sa}",
        "spring.datasource.password=${l17.jdbc.password:}",
        "spring.jpa.hibernate.ddl-auto=${l17.ddl:create-drop}",
        "spring.flyway.enabled=${l17.flyway:false}",
        "spring.sql.init.mode=never"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class SagaIdsAwaitingAnalysisTest {

    private static final List<SagaStatus> ENDED_WITHOUT_SUCCESS =
            List.of(SagaStatus.FAILED, SagaStatus.COMPENSATED);
    /** Source clock: days before the platform read anything. */
    private static final Instant STAMPED = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant RECEIVED = Instant.parse("2026-01-05T00:00:00Z");

    @Autowired private SagaInstanceRepository sagas;
    @Autowired private SagaEventRepository events;
    @Autowired private RcaResultRepository results;
    @Autowired private AnomalyRepository anomalies;

    @BeforeEach
    void clean() {
        anomalies.deleteAll();
        results.deleteAll();
        events.deleteAll();
        sagas.deleteAll();
    }

    /**
     * The first live hotel run: stamped 16:03, read 16:05. A window on last_updated, the
     * source's clock, would have missed it; received_at does not.
     */
    @Test
    void anEndedRunReadLongAfterItsSourceStampedItIsPickedUp() {
        String failed = run(SagaStatus.FAILED, RECEIVED);
        String compensated = run(SagaStatus.COMPENSATED, RECEIVED);

        assertThat(awaiting()).containsExactlyInAnyOrder(failed, compensated);
    }

    @Test
    void aRunAnalysedAfterItsLastObservationIsNotPickedUpAgain() {
        String id = run(SagaStatus.FAILED, RECEIVED);
        analysed(id, RECEIVED.plusSeconds(1));

        assertThat(awaiting()).isEmpty();
    }

    /** Late: evidence received after the analysis makes that analysis stale. */
    @Test
    void anObservationReceivedAfterTheAnalysisMakesTheRunDueAgain() {
        String id = run(SagaStatus.FAILED, RECEIVED);
        analysed(id, RECEIVED.plusSeconds(1));
        observe(id, "late", RECEIVED.plusSeconds(2));

        assertThat(awaiting()).containsExactly(id);
    }

    /** Healthy and unfinished runs belong to the finding-driven pass, not this one. */
    @Test
    void completedAndRunningRunsAreLeftAlone() {
        run(SagaStatus.COMPLETED, RECEIVED);
        run(SagaStatus.IN_PROGRESS, RECEIVED);
        run(SagaStatus.COMPENSATING, RECEIVED);

        assertThat(awaiting()).isEmpty();
    }

    /**
     * A run that went quiet, was diagnosed while waiting, and then completed: the diagnosis
     * was made on half the evidence. Nothing new was detected when it completed (a source
     * without per-step completion events gives no step timeout), so the finding-driven pass,
     * which waits for a new finding, never looked again (FTGO pause runs 7 and 13: R06 never
     * assessed). Completed runs with findings are due again when evidence outdates the analysis.
     */
    @Test
    void aCompletedRunWithFindingsIsDueWhenEvidenceArrivesAfterItsDiagnosis() {
        String recovered = run(SagaStatus.COMPLETED, RECEIVED);
        finding(recovered, RECEIVED.plusSeconds(1));
        analysed(recovered, RECEIVED.plusSeconds(2));
        observe(recovered, "completed", RECEIVED.plusSeconds(3));

        assertThat(completedWithFindingsAwaiting()).containsExactly(recovered);
    }

    @Test
    void aCompletedRunWithoutFindingsOrWithAFreshDiagnosisIsLeftAlone() {
        run(SagaStatus.COMPLETED, RECEIVED);                       // healthy: nothing to diagnose
        String fresh = run(SagaStatus.COMPLETED, RECEIVED);
        finding(fresh, RECEIVED.plusSeconds(1));
        analysed(fresh, RECEIVED.plusSeconds(2));                  // already saw everything

        assertThat(completedWithFindingsAwaiting()).isEmpty();
    }

    /**
     * EXT-OVERLOAD-FALSE-FINDINGS: a diagnosis written after the last observation but before
     * a finding it cited was withdrawn is stale, and the finding-driven pass has to see it.
     */
    @Test
    void aWithdrawalAfterTheDiagnosisCountsAsAChangedFinding() {
        String id = run(SagaStatus.COMPLETED, RECEIVED);
        Anomaly orphan = new Anomaly();
        orphan.setSagaId(id);
        orphan.setAnomalyType(AnomalyType.ORPHAN_EVENT);
        orphan.setDetectedAt(RECEIVED.minusSeconds(1));
        orphan.setRetractedAt(RECEIVED.plusSeconds(2));
        anomalies.save(orphan);
        analysed(id, RECEIVED.plusSeconds(1));

        assertThat(anomalies.findLatestDetectedAtBySagaId(id)).isEqualTo(RECEIVED.plusSeconds(2));
        assertThat(anomalies.findSagaIdsWithAnomaliesSince(RECEIVED.plusSeconds(2))).containsExactly(id);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private List<String> completedWithFindingsAwaiting() {
        return sagas.findSagaIdsWithFindingsAwaitingAnalysis(List.of(SagaStatus.COMPLETED), PageRequest.of(0, 50));
    }

    private void finding(String sagaId, Instant at) {
        Anomaly anomaly = new Anomaly();
        anomaly.setSagaId(sagaId);
        anomaly.setAnomalyType(AnomalyType.SAGA_STUCK);
        anomaly.setDetectedAt(at);
        anomalies.save(anomaly);
    }

    private List<String> awaiting() {
        return sagas.findSagaIdsAwaitingAnalysis(ENDED_WITHOUT_SUCCESS, PageRequest.of(0, 50));
    }

    private String run(SagaStatus status, Instant receivedAt) {
        String id = "default::" + UUID.randomUUID();
        SagaInstance saga = new SagaInstance();
        saga.setSagaId(id);
        saga.setStatus(status);
        saga.setStartTime(STAMPED);
        saga.setLastUpdated(STAMPED);
        sagas.save(saga);
        observe(id, "first", receivedAt);
        return id;
    }

    private void observe(String sagaId, String eventId, Instant receivedAt) {
        SagaEvent event = new SagaEvent();
        event.setSagaId(sagaId);
        event.setSourceName("default");
        event.setEventId(sagaId + "-" + eventId);
        event.setEventType("PAYMENT_FAILED");
        event.setTimestamp(STAMPED);
        event.setPayload("{}");
        event.setReceivedAt(receivedAt);
        events.save(event);
    }

    private void analysed(String sagaId, Instant at) {
        RcaResult result = new RcaResult();
        result.setSagaId(sagaId);
        result.setOutcome("FAILED");
        result.setSeverity("LOW");
        result.setEvidence("[]");
        result.setCreatedAt(at);
        results.save(result);
    }
}
