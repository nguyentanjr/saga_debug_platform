package com.example.correlation.domain;

import java.util.Objects;

public record ExecutionKey(String source, String rawId) {
    public static final String SEPARATOR = "::";

    public static final String DEFAULT_SOURCE = "default";

    public ExecutionKey {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(rawId, "rawId");
        if (source.isBlank()) {
            throw new IllegalArgumentException("source must not be blank");
        }
        if (rawId.isBlank()) {
            throw new IllegalArgumentException("rawId must not be blank");
        }
        if (source.contains(SEPARATOR)) {
            throw new IllegalArgumentException(
                    "source name must not contain '" + SEPARATOR + "': " + source);
        }
    }

    public static ExecutionKey of(String source, String rawId) {
        return new ExecutionKey(source, rawId);
    }

    public static ExecutionKey parse(String canonical) {
        if (canonical == null || canonical.isBlank()) {
            throw new IllegalArgumentException("no execution key to parse");
        }
        int at = canonical.indexOf(SEPARATOR);
        if (at < 0) {
            throw new IllegalArgumentException(
                    "not a namespaced execution key: '" + canonical + "'");
        }
        return new ExecutionKey(canonical.substring(0, at),
                canonical.substring(at + SEPARATOR.length()));
    }

    public static String sourceOf(String canonical) {
        if (canonical == null) return null;
        int at = canonical.indexOf(SEPARATOR);
        return at < 0 ? null : canonical.substring(0, at);
    }

    public boolean sameSourceAs(ExecutionKey other) {
        return other != null && source.equals(other.source);
    }

    public String canonical() {
        return source + SEPARATOR + rawId;
    }

    @Override
    public String toString() {
        return canonical();
    }
}
