package com.example.correlation.ingestion;

import com.example.correlation.domain.ExecutionKey;

import java.time.Instant;

public class PassthroughAliasResolver extends ExecutionAliasResolver {
    public PassthroughAliasResolver() {
        super(null, null, null, null, null);
    }

    @Override
    public ExecutionKey canonical(ExecutionKey declared, ExecutionKey alias,
            String eventId, Instant observedAt) {
        return declared;
    }
}
