package com.example.correlation.execution;

import com.example.correlation.domain.ExecutionAlias;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

@Component
public class ExecutionLookup {
    @ResponseStatus(HttpStatus.CONFLICT)
    public static class AmbiguousExecutionException extends RuntimeException {
        private final transient Set<String> candidates;

        AmbiguousExecutionException(String rawId, Set<String> candidates) {
            super("'" + rawId + "' names an execution in more than one observed system: "
                    + candidates + ". Ask for one of these keys instead.");
            this.candidates = candidates;
        }

        public Set<String> candidates() {
            return candidates;
        }
    }

    private final SagaInstanceRepository sagaInstances;
    private final ExecutionAliasRepository aliases;
    private final SourceMappingRegistry sourceMappings;

    public ExecutionLookup(SagaInstanceRepository sagaInstances,
            ExecutionAliasRepository aliases,
            SourceMappingRegistry sourceMappings) {
        this.sagaInstances = sagaInstances;
        this.aliases = aliases;
        this.sourceMappings = sourceMappings;
    }

    public Optional<String> resolve(String given) {
        if (given == null || given.isBlank()) {
            return Optional.empty();
        }

        if (given.contains(ExecutionKey.SEPARATOR)) {
            return executionUnder(ExecutionKey.parse(given));
        }

        Set<String> found = new LinkedHashSet<>();
        for (String source : sourceMappings.knownSourceNames()) {
            executionUnder(ExecutionKey.of(source, given)).ifPresent(found::add);
        }

        if (found.isEmpty()) {
            return Optional.empty();
        }
        if (found.size() > 1) {
            throw new AmbiguousExecutionException(given, found);
        }
        return Optional.of(found.iterator().next());
    }

    private Optional<String> executionUnder(ExecutionKey key) {
        if (sagaInstances.existsById(key.canonical())) {
            return Optional.of(key.canonical());
        }
        return aliases.findById(ExecutionAlias.Key.of(key))
                .map(ExecutionAlias::getExecutionId)
                .filter(sagaInstances::existsById);
    }
}
