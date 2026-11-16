package com.example.correlation.scheduling;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class SweepLease {
    private static final Logger log = LoggerFactory.getLogger(SweepLease.class);

    private static final String DB_EPOCH_NOW = "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT)";

    private static final String TAKE_OVER = """
            UPDATE scheduler_lease
               SET holder = ?, token = ?, taken_at = CURRENT_TIMESTAMP,
                   lease_expires_at = %s + ?
             WHERE sweep = ? AND lease_expires_at <= %s
            """.formatted(DB_EPOCH_NOW, DB_EPOCH_NOW);

    private static final String TAKE_FIRST = """
            INSERT INTO scheduler_lease (sweep, holder, token, taken_at, lease_expires_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, %s + ?)
            """.formatted(DB_EPOCH_NOW);

    private static final String RELEASE = """
            UPDATE scheduler_lease
               SET lease_expires_at = %s
             WHERE sweep = ? AND token = ?
            """.formatted(DB_EPOCH_NOW);

    private final JdbcTemplate jdbc;
    private final String holder;

    @Autowired
    public SweepLease(JdbcTemplate jdbc) {
        this(jdbc, ManagementFactory.getRuntimeMXBean().getName());
    }

    SweepLease(JdbcTemplate jdbc, String holder) {
        this.jdbc = jdbc;
        this.holder = holder;
    }

    public boolean runExclusively(String sweep, Duration lease, Runnable work) {
        String token = UUID.randomUUID().toString();
        if (!take(sweep, token, lease)) {
            log.debug("Sweep {} skipped: held elsewhere", sweep);
            return false;
        }
        try {
            work.run();
            return true;
        } finally {
            release(sweep, token);
        }
    }

    private boolean take(String sweep, String token, Duration lease) {
        long seconds = Math.max(1, lease.toSeconds());
        if (jdbc.update(TAKE_OVER, holder, token, seconds, sweep) == 1) {
            return true;
        }

        try {
            jdbc.update(TAKE_FIRST, sweep, holder, token, seconds);
            return true;
        } catch (DuplicateKeyException alreadyHeld) {
            return false;
        }
    }

    private void release(String sweep, String token) {
        try {
            jdbc.update(RELEASE, sweep, token);
        } catch (RuntimeException e) {
            log.warn("Could not release lease on sweep {}; it will expire on its own", sweep, e);
        }
    }
}
