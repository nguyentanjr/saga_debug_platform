package com.example.correlation.scheduling;

import java.time.Duration;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.RetentionService;

@Component
public class ScheduledSweeps {
    private static final Duration DETECTION_LEASE = Duration.ofMinutes(2);

    private static final Duration RCA_LEASE = Duration.ofMinutes(5);

    private static final Duration RETENTION_LEASE = Duration.ofMinutes(30);

    private final SweepLease lease;
    private final AnomalyDetectionService detection;
    private final RcaService rca;

    private final ObjectProvider<RetentionService> retention;

    public ScheduledSweeps(SweepLease lease, AnomalyDetectionService detection,
                           RcaService rca, ObjectProvider<RetentionService> retention) {
        this.lease = lease;
        this.detection = detection;
        this.rca = rca;
        this.retention = retention;
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectStuckSagas() {
        lease.runExclusively("detect-stuck-sagas", DETECTION_LEASE, detection::detectStuckSagas);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectStepTimeouts() {
        lease.runExclusively("detect-step-timeouts", DETECTION_LEASE, detection::detectStepTimeouts);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingTerminalEvents() {
        lease.runExclusively("detect-missing-terminal-events", DETECTION_LEASE,
                detection::detectMissingTerminalEvents);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectCompensationMissing() {
        lease.runExclusively("detect-compensation-missing", DETECTION_LEASE,
                detection::detectCompensationMissing);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingCompensationAck() {
        lease.runExclusively("detect-missing-compensation-ack", DETECTION_LEASE,
                detection::detectMissingCompensationAck);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectDelayedCompensation() {
        lease.runExclusively("detect-delayed-compensation", DETECTION_LEASE,
                detection::detectDelayedCompensation);
    }

    @Scheduled(fixedRateString = "${rca.auto-analysis.fixed-rate-ms:30000}")
    public void autoAnalyzeNewlyDetectedAnomalies() {
        lease.runExclusively("auto-analyze-anomalies", RCA_LEASE,
                rca::autoAnalyzeNewlyDetectedAnomalies);
    }

    @Scheduled(fixedRateString = "${rca.auto-analysis.fixed-rate-ms:30000}")
    public void autoAnalyzeEndedRuns() {
        lease.runExclusively("auto-analyze-ended-runs", RCA_LEASE,
                rca::autoAnalyzeEndedRuns);
    }

    @Scheduled(cron = "${retention.schedule-cron:0 30 3 * * *}")
    public void purgeExpiredSagas() {
        RetentionService enabled = retention.getIfAvailable();
        if (enabled == null) return;
        lease.runExclusively("purge-expired-sagas", RETENTION_LEASE, enabled::purgeExpiredSagas);
    }
}
