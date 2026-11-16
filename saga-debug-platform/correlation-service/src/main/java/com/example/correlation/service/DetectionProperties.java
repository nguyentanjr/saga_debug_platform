package com.example.correlation.service;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "anomaly.detection")
public class DetectionProperties {
    private Duration clockSkewTolerance = Duration.ofMillis(500);
    private int retryBurstThreshold = 3;
    private Duration retryBurstWindow = Duration.ofSeconds(60);
    private long scheduleRateMs = 15_000;
    private int defaultStepTimeoutSeconds = 60;
    private Duration completedSagaLookback = Duration.ofHours(1);
    private int missingTerminalTimeoutMultiplier = 10;
    private Duration compensationMissingCutoff = Duration.ofSeconds(30);
    private Duration compensationAckCutoff = Duration.ofSeconds(30);
    private double delayedCompensationMultiplier = 1.5;
    private Duration compensationRecentLookback = Duration.ofMinutes(10);
    private int serviceCrashTimeoutMultiplier = 3;
    private StepTimeoutMode stepTimeoutMode = StepTimeoutMode.PROJECTION;
    private int detectionBatchSize = 500;

    public int getDetectionBatchSize() { return detectionBatchSize; }
    public void setDetectionBatchSize(int detectionBatchSize) { this.detectionBatchSize = detectionBatchSize; }

    public enum StepTimeoutMode { LEGACY, SHADOW, PROJECTION }

    public StepTimeoutMode getStepTimeoutMode() { return stepTimeoutMode; }
    public void setStepTimeoutMode(StepTimeoutMode stepTimeoutMode) { this.stepTimeoutMode = stepTimeoutMode; }

    public Duration getClockSkewTolerance() { return clockSkewTolerance; }
    public void setClockSkewTolerance(Duration clockSkewTolerance) { this.clockSkewTolerance = clockSkewTolerance; }

    public int getRetryBurstThreshold() { return retryBurstThreshold; }
    public void setRetryBurstThreshold(int retryBurstThreshold) { this.retryBurstThreshold = retryBurstThreshold; }

    public Duration getRetryBurstWindow() { return retryBurstWindow; }
    public void setRetryBurstWindow(Duration retryBurstWindow) { this.retryBurstWindow = retryBurstWindow; }

    public long getScheduleRateMs() { return scheduleRateMs; }
    public void setScheduleRateMs(long scheduleRateMs) { this.scheduleRateMs = scheduleRateMs; }

    public int getDefaultStepTimeoutSeconds() { return defaultStepTimeoutSeconds; }
    public void setDefaultStepTimeoutSeconds(int defaultStepTimeoutSeconds) {
        this.defaultStepTimeoutSeconds = defaultStepTimeoutSeconds;
    }

    public Duration getCompletedSagaLookback() { return completedSagaLookback; }
    public void setCompletedSagaLookback(Duration completedSagaLookback) {
        this.completedSagaLookback = completedSagaLookback;
    }

    public int getMissingTerminalTimeoutMultiplier() { return missingTerminalTimeoutMultiplier; }
    public void setMissingTerminalTimeoutMultiplier(int missingTerminalTimeoutMultiplier) {
        this.missingTerminalTimeoutMultiplier = missingTerminalTimeoutMultiplier;
    }

    public Duration getCompensationMissingCutoff() { return compensationMissingCutoff; }
    public void setCompensationMissingCutoff(Duration compensationMissingCutoff) {
        this.compensationMissingCutoff = compensationMissingCutoff;
    }

    public Duration getCompensationAckCutoff() { return compensationAckCutoff; }
    public void setCompensationAckCutoff(Duration compensationAckCutoff) {
        this.compensationAckCutoff = compensationAckCutoff;
    }

    public double getDelayedCompensationMultiplier() { return delayedCompensationMultiplier; }
    public void setDelayedCompensationMultiplier(double delayedCompensationMultiplier) {
        this.delayedCompensationMultiplier = delayedCompensationMultiplier;
    }

    public Duration getCompensationRecentLookback() { return compensationRecentLookback; }
    public void setCompensationRecentLookback(Duration compensationRecentLookback) {
        this.compensationRecentLookback = compensationRecentLookback;
    }

    public int getServiceCrashTimeoutMultiplier() { return serviceCrashTimeoutMultiplier; }
    public void setServiceCrashTimeoutMultiplier(int serviceCrashTimeoutMultiplier) {
        this.serviceCrashTimeoutMultiplier = serviceCrashTimeoutMultiplier;
    }
}
