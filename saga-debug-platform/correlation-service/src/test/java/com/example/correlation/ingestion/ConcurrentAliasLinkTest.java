package com.example.correlation.ingestion;

import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two halves of one run reaching the resolver at the same moment (EXT-ALIAS-RACE).
 *
 * <p>Seen on FTGO: OrderCreated (keyed by the order id) and the first saga command (keyed
 * by the saga id, carrying the order id as its alias) were ingested ~3 ms apart on two
 * consumer threads. The command's worker looked for an execution under the order id before
 * the event's worker had committed it, found none, linked the order id to the saga id and
 * moved on; the order-id execution committed a moment later and was never folded in. It
 * stayed IN_PROGRESS with a false SAGA_STUCK while its other half was blamed on a wrong
 * correlation key.
 *
 * <p>Real transactions on a real database, because the defect lives in what one
 * transaction cannot yet see of another.
 */
@DataJpaTest(showSql = false, properties = {
        "spring.datasource.url=jdbc:h2:mem:aliasrace;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false",
        "spring.sql.init.mode=never"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class ConcurrentAliasLinkTest {

    private static final String SYSTEM = "ftgo";
    private static final Instant T0 = Instant.parse("2026-10-16T08:18:35Z");

    @Autowired private ExecutionAliasRepository aliases;
    @Autowired private SagaInstanceRepository instances;
    @Autowired private SagaEventRepository events;
    @Autowired private AnomalyRepository anomalies;
    @Autowired private RcaResultRepository rcaResults;
    @Autowired private PlatformTransactionManager transactionManager;

    private ExecutionAliasResolver resolver;
    private TransactionTemplate tx;
    private ExecutionKey order;
    private ExecutionKey saga;

    @BeforeEach
    void setUp() {
        resolver = new ExecutionAliasResolver(aliases, events, instances, anomalies, rcaResults);
        tx = new TransactionTemplate(transactionManager);
        String run = UUID.randomUUID().toString().substring(0, 8);
        order = ExecutionKey.of(SYSTEM, "order-" + run);
        saga = ExecutionKey.of(SYSTEM, "saga-" + run);
    }

    @Test
    void anOrderEventAndASagaCommandIngestedTogetherEndAsOneExecution() throws Exception {
        CountDownLatch orderWritten = new CountDownLatch(1);

        // Worker A: the order event, which carries only the order id. Holds its transaction
        // open briefly after writing, so worker B runs while A's execution is uncommitted.
        CompletableFuture<Void> orderWorker = CompletableFuture.runAsync(() -> withRetry(() -> tx.executeWithoutResult(s -> {
            ExecutionKey key = resolver.canonical(order, null, "order-created", T0);
            createIfAbsent(key, T0);
            orderWritten.countDown();
            sleep(500);
        })));

        // Worker B: the saga command, carrying both ids. Either worker may be the one whose
        // commit collides; each is retried, as the Kafka error handler retries a record
        // whose transaction did not commit.
        assertThat(orderWritten.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> commandWorker = CompletableFuture.runAsync(() -> withRetry(() -> tx.executeWithoutResult(s -> {
            ExecutionKey key = resolver.canonical(saga, order, "validate-requested", T0.plusMillis(3));
            createIfAbsent(key, T0.plusMillis(3));
        })));

        orderWorker.get(30, TimeUnit.SECONDS);
        commandWorker.get(30, TimeUnit.SECONDS);

        ExecutionKey viaOrder = tx.execute(s -> resolver.canonical(order, null, "order-authorized", T0.plusSeconds(1)));
        ExecutionKey viaSaga = tx.execute(s -> resolver.canonical(saga, null, "approve-requested", T0.plusSeconds(1)));

        assertThat(viaOrder).as("both identifiers name the same execution").isEqualTo(viaSaga);
        ExecutionKey other = viaOrder.equals(order) ? saga : order;
        assertThat(instances.findById(other.canonical()))
                .as("no second execution is left behind under " + other.canonical())
                .isEmpty();
        assertThat(instances.findById(viaOrder.canonical())).isPresent();
    }

    @Test
    void anExecutionSeenOnceWithoutALinkStillResolvesToItself() {
        ExecutionKey key = tx.execute(s -> resolver.canonical(order, null, "order-created", T0));
        ExecutionKey again = tx.execute(s -> resolver.canonical(order, null, "order-created-replay", T0));

        assertThat(key).isEqualTo(order);
        assertThat(again).isEqualTo(order);
    }

    private void createIfAbsent(ExecutionKey key, Instant at) {
        if (instances.findById(key.canonical()).isPresent()) return;
        SagaInstance instance = new SagaInstance();
        instance.setSagaId(key.canonical());
        instance.setSourceName(key.source());
        instance.setStatus(SagaStatus.IN_PROGRESS);
        instance.setStartTime(at);
        instance.setLastUpdated(at);
        instances.save(instance);
    }

    /** At most three attempts, the way a redelivered record is processed again. */
    private static void withRetry(Runnable attempt) {
        for (int n = 1; ; n++) {
            try {
                attempt.run();
                return;
            } catch (RuntimeException collision) {
                if (n == 3) throw collision;
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
