package com.example.correlation.domain;

public enum SagaStatus {
    STARTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    COMPENSATING,
    COMPENSATED;

    public int progressRank() {
        return switch (this) {
            case STARTED -> 1;
            case IN_PROGRESS -> 2;
            case COMPENSATING -> 3;
            case FAILED -> 4;
            case COMPENSATED -> 5;
            case COMPLETED -> 6;
        };
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == COMPENSATED;
    }
}
