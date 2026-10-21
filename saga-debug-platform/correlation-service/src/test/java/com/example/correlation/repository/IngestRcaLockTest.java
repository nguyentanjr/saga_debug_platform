package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXT-OVERLOAD-FALSE-FINDINGS: an analysis must not read a finding an ingest is withdrawing.
 *
 * <p>Bench 2000 ev/s (856447a): 10 runs kept WRONG_SAGA_CORRELATION_KEY for good. Ingest only
 * version-checked the run row, so RCA -- which locks it -- read ORPHAN_EVENT as standing while
 * the ingest that withdrew it had not committed, then stamped its diagnosis after the
 * withdrawal; the stale-diagnosis sweep saw nothing newer. Ingest now takes the same lock.
 * Real transactions, because the defect is what one transaction cannot yet see of another.
 */
@DataJpaTest(showSql = false, properties = {
        "spring.datasource.url=jdbc:h2:mem:ingestrca;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false",
        "spring.sql.init.mode=never"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class IngestRcaLockTest {

    @Autowired private SagaInstanceRepository sagas;
    @Autowired private AnomalyRepository anomalies;
    @Autowired private PlatformTransactionManager transactionManager;

    @Test
    void anAnalysisWaitsForTheIngestWithdrawingItsFindings() throws Exception {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        String id = "default::" + UUID.randomUUID();
        tx.executeWithoutResult(s -> {
            SagaInstance run = new SagaInstance();
            run.setSagaId(id);
            run.setStatus(SagaStatus.COMPLETED);
            run.setStartTime(Instant.now());
            run.setLastUpdated(Instant.now());
            sagas.save(run);
            Anomaly orphan = new Anomaly();
            orphan.setSagaId(id);
            orphan.setAnomalyType(AnomalyType.ORPHAN_EVENT);
            anomalies.save(orphan);
        });

        CountDownLatch withdrawn = new CountDownLatch(1);
        CountDownLatch commit = new CountDownLatch(1);
        CompletableFuture<Void> ingest = CompletableFuture.runAsync(() -> tx.executeWithoutResult(s -> {
            sagas.findByIdForUpdate(id);
            anomalies.retractClaims(id, Set.of(AnomalyType.ORPHAN_EVENT), Instant.now(), "start arrived");
            withdrawn.countDown();
            await(commit);
        }));
        assertThat(withdrawn.await(10, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<List<AnomalyType>> analysis = CompletableFuture.supplyAsync(() -> tx.execute(s -> {
            sagas.findByIdForUpdate(id);
            return anomalies.findStandingBySagaId(id).stream().map(Anomaly::getAnomalyType).toList();
        }));
        Thread.sleep(300);
        assertThat(analysis).as("the analysis waits while the ingest holds the run").isNotDone();

        commit.countDown();
        ingest.get(10, TimeUnit.SECONDS);
        assertThat(analysis.get(10, TimeUnit.SECONDS)).doesNotContain(AnomalyType.ORPHAN_EVENT);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
