package com.example.correlation.ingestion;

public record IngestionResult(Outcome outcome, String eventId, String reason) {
    public enum Outcome {
        ACCEPTED,

        DUPLICATE,

        UNREADABLE,

        NOT_CORRELATABLE
    }

    public static IngestionResult accepted(String eventId) {
        return new IngestionResult(Outcome.ACCEPTED, eventId, null);
    }

    public static IngestionResult duplicate(String eventId) {
        return new IngestionResult(Outcome.DUPLICATE, eventId, null);
    }

    public static IngestionResult unreadable(String reason) {
        return new IngestionResult(Outcome.UNREADABLE, null, reason);
    }

    public static IngestionResult notCorrelatable(String reason) {
        return new IngestionResult(Outcome.NOT_CORRELATABLE, null, reason);
    }

    public boolean isAccepted() {
        return outcome == Outcome.ACCEPTED;
    }

    public boolean isPermanentlyRejected() {
        return outcome == Outcome.UNREADABLE || outcome == Outcome.NOT_CORRELATABLE;
    }
}
