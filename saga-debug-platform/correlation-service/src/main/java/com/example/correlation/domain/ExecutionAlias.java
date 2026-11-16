package com.example.correlation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

@Entity
@Table(name = "execution_aliases")
@IdClass(ExecutionAlias.Key.class)
public class ExecutionAlias {
    @Id
    @Column(name = "source_name")
    private String sourceName;

    @Id
    @Column(name = "alias_key")
    private String aliasKey;

    @Column(name = "execution_id", nullable = false)
    private String executionId;

    @Column(name = "linked_by_event_id")
    private String linkedByEventId;

    @Column(name = "observed_at", nullable = false)
    private Instant observedAt;

    protected ExecutionAlias() {
    }

    public ExecutionAlias(String sourceName, String aliasKey, String executionId,
            String linkedByEventId, Instant observedAt) {
        this.sourceName = sourceName;
        this.aliasKey = aliasKey;
        this.executionId = executionId;
        this.linkedByEventId = linkedByEventId;
        this.observedAt = observedAt;
    }

    public String getSourceName() { return sourceName; }

    public String getAliasKey() { return aliasKey; }

    public String getExecutionId() { return executionId; }
    public void setExecutionId(String executionId) { this.executionId = executionId; }

    public String getLinkedByEventId() { return linkedByEventId; }

    public Instant getObservedAt() { return observedAt; }

    public ExecutionKey key() {
        return ExecutionKey.of(sourceName, aliasKey);
    }

    public static class Key implements Serializable {
        private String sourceName;
        private String aliasKey;

        public Key() {
        }

        public Key(String sourceName, String aliasKey) {
            this.sourceName = sourceName;
            this.aliasKey = aliasKey;
        }

        public static Key of(ExecutionKey key) {
            return new Key(key.source(), key.rawId());
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Key that)) return false;
            return Objects.equals(sourceName, that.sourceName)
                    && Objects.equals(aliasKey, that.aliasKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceName, aliasKey);
        }
    }
}
