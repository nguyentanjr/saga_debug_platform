package com.example.correlation.scheduling;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class SweepLeaseTest {
    private static final String SWEEP = "detect-stuck-sagas";

    private DataSource dataSource;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:lease-" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "sa", "");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
                CREATE TABLE scheduler_lease (
                    sweep            VARCHAR(64)  PRIMARY KEY,
                    holder           VARCHAR(128) NOT NULL,
                    token            VARCHAR(36)  NOT NULL,
                    taken_at         TIMESTAMP WITH TIME ZONE NOT NULL,
                    lease_expires_at BIGINT       NOT NULL
                )
                """);
    }

    @AfterEach
    void tearDown() {
        jdbc.execute("DROP ALL OBJECTS");
    }

    private SweepLease leaseHeldBy(String holder) {
        return new SweepLease(jdbc, holder);
    }

    @Test
    @DisplayName("the first instance to ask for a sweep that has never run gets it")
    void firstEverSweepIsGranted() {
        AtomicInteger ran = new AtomicInteger();

        boolean granted = leaseHeldBy("instance-a")
                .runExclusively(SWEEP, Duration.ofMinutes(2), ran::incrementAndGet);

        assertThat(granted).isTrue();
        assertThat(ran).hasValue(1);
        assertThat(jdbc.queryForObject("SELECT holder FROM scheduler_lease WHERE sweep = ?",
                String.class, SWEEP)).isEqualTo("instance-a");
    }

    @Test
    @DisplayName("a second instance does not run a sweep while the first still holds it")
    void aHeldSweepIsSkippedElsewhere() {
        AtomicInteger ranOnB = new AtomicInteger();
        CountDownLatch bTried = new CountDownLatch(1);

        boolean grantedToA = leaseHeldBy("instance-a").runExclusively(SWEEP, Duration.ofMinutes(2), () -> {
            boolean grantedToB = leaseHeldBy("instance-b")
                    .runExclusively(SWEEP, Duration.ofMinutes(2), ranOnB::incrementAndGet);
            assertThat(grantedToB).isFalse();
            bTried.countDown();
        });

        assertThat(grantedToA).isTrue();
        assertThat(bTried.getCount()).isZero();
        assertThat(ranOnB).hasValue(0);
    }

    @Test
    @DisplayName("under a concurrent rush the work runs exactly once")
    void concurrentInstancesProduceOneRun() throws Exception {
        int instances = 8;
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch startTogether = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(instances);

        try {
            List<Future<Boolean>> attempts = IntStream.range(0, instances)
                    .mapToObj(i -> pool.submit(() -> {
                        startTogether.await();
                        return leaseHeldBy("instance-" + i).runExclusively(SWEEP, Duration.ofMinutes(2), () -> {
                            runs.incrementAndGet();
                            sleepBriefly();
                        });
                    }))
                    .toList();

            startTogether.countDown();
            long granted = attempts.stream().map(SweepLeaseTest::get).filter(Boolean::booleanValue).count();

            assertThat(granted).isEqualTo(1);
            assertThat(runs).hasValue(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("releasing lets the very next tick run, without waiting out the lease")
    void releaseMakesTheSweepImmediatelyAvailable() {
        AtomicInteger runs = new AtomicInteger();
        SweepLease a = leaseHeldBy("instance-a");

        assertThat(a.runExclusively(SWEEP, Duration.ofMinutes(30), runs::incrementAndGet)).isTrue();
        assertThat(leaseHeldBy("instance-b")
                .runExclusively(SWEEP, Duration.ofMinutes(30), runs::incrementAndGet)).isTrue();

        assertThat(runs).hasValue(2);
    }

    @Test
    @DisplayName("a holder that dies mid-sweep blocks the others only until its lease expires")
    void anExpiredLeaseIsTakenOver() {
        AtomicInteger ran = new AtomicInteger();

        jdbc.update("""
                INSERT INTO scheduler_lease (sweep, holder, token, taken_at, lease_expires_at)
                VALUES (?, 'instance-gone', 'token-gone', CURRENT_TIMESTAMP,
                        CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT) - 1)
                """, SWEEP);

        boolean granted = leaseHeldBy("instance-b")
                .runExclusively(SWEEP, Duration.ofMinutes(2), ran::incrementAndGet);

        assertThat(granted).isTrue();
        assertThat(ran).hasValue(1);
    }

    @Test
    @DisplayName("an unexpired lease left by a dead holder is still respected")
    void anUnexpiredLeaseIsRespected() {
        AtomicInteger ran = new AtomicInteger();
        jdbc.update("""
                INSERT INTO scheduler_lease (sweep, holder, token, taken_at, lease_expires_at)
                VALUES (?, 'instance-gone', 'token-gone', CURRENT_TIMESTAMP,
                        CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT) + 600)
                """, SWEEP);

        boolean granted = leaseHeldBy("instance-b")
                .runExclusively(SWEEP, Duration.ofMinutes(2), ran::incrementAndGet);

        assertThat(granted).isFalse();
        assertThat(ran).hasValue(0);
    }

    @Test
    @DisplayName("a sweep that overran does not free the lease its successor now holds")
    void releaseOnlyFreesItsOwnAcquisition() {
        AtomicInteger bHolds = new AtomicInteger();
        leaseHeldBy("instance-a").runExclusively(SWEEP, Duration.ofMinutes(2), () -> {
            jdbc.update("""
                    UPDATE scheduler_lease
                       SET holder = 'instance-b', token = 'token-b',
                           lease_expires_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT) + 600
                     WHERE sweep = ?
                    """, SWEEP);
            bHolds.incrementAndGet();
        });

        assertThat(bHolds).hasValue(1);

        Long expiresAt = jdbc.queryForObject(
                "SELECT lease_expires_at - CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT)"
                        + " FROM scheduler_lease WHERE sweep = ?", Long.class, SWEEP);
        assertThat(expiresAt).isGreaterThan(0L);
        assertThat(jdbc.queryForObject("SELECT holder FROM scheduler_lease WHERE sweep = ?",
                String.class, SWEEP)).isEqualTo("instance-b");
    }

    @Test
    @DisplayName("a sweep that throws still releases, so one bad pass does not stop the rest")
    void failingWorkStillReleases() {
        AtomicInteger runs = new AtomicInteger();

        try {
            leaseHeldBy("instance-a").runExclusively(SWEEP, Duration.ofMinutes(30), () -> {
                runs.incrementAndGet();
                throw new IllegalStateException("detector blew up");
            });
        } catch (IllegalStateException expected) {
        }

        assertThat(leaseHeldBy("instance-b")
                .runExclusively(SWEEP, Duration.ofMinutes(30), runs::incrementAndGet)).isTrue();
        assertThat(runs).hasValue(2);
    }

    @Test
    @DisplayName("different sweeps do not contend with each other")
    void leasesAreScopedToOneSweep() {
        AtomicInteger runs = new AtomicInteger();

        boolean granted = leaseHeldBy("instance-a").runExclusively(SWEEP, Duration.ofMinutes(2), () -> {
            runs.incrementAndGet();
            assertThat(leaseHeldBy("instance-b")
                    .runExclusively("purge-expired-sagas", Duration.ofMinutes(2), runs::incrementAndGet))
                    .isTrue();
        });

        assertThat(granted).isTrue();
        assertThat(runs).hasValue(2);
    }

    @Test
    @DisplayName("the expiry the lease writes is measured by the database, not by the caller")
    void expiryComesFromTheDatabaseClock() {
        leaseHeldBy("instance-a").runExclusively("purge-expired-sagas", Duration.ofMinutes(10), () -> {
            Long remaining = jdbc.queryForObject(
                    "SELECT lease_expires_at - CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT)"
                            + " FROM scheduler_lease WHERE sweep = ?", Long.class, "purge-expired-sagas");

            assertThat(remaining).isBetween(590L, 600L);
        });
    }

    private static void sleepBriefly() {
        try {
            TimeUnit.MILLISECONDS.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean get(Future<Boolean> future) {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
