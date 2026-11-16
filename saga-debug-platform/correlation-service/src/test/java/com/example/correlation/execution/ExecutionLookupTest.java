package com.example.correlation.execution;

import com.example.correlation.domain.ExecutionAlias;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExecutionLookupTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private Map<String, SagaInstance> instances;
    private Map<ExecutionAlias.Key, ExecutionAlias> aliasRows;
    private Set<String> sourceNames;

    private ExecutionLookup lookup;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        aliasRows = new LinkedHashMap<>();
        sourceNames = new LinkedHashSet<>();
        sourceNames.add(ExecutionKey.DEFAULT_SOURCE);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.existsById(anyString()))
                .thenAnswer(call -> instances.containsKey(call.<String>getArgument(0)));

        ExecutionAliasRepository aliases = mock(ExecutionAliasRepository.class);
        when(aliases.findById(any(ExecutionAlias.Key.class)))
                .thenAnswer(call -> Optional.ofNullable(aliasRows.get(call.getArgument(0))));

        SourceMappingRegistry registry = mock(SourceMappingRegistry.class);
        when(registry.knownSourceNames()).thenAnswer(call -> sourceNames);

        lookup = new ExecutionLookup(sagaInstances, aliases, registry);
    }

    @Test
    void aStoredKeyResolvesToItself() {
        run("default::ORD-1");

        assertThat(lookup.resolve("default::ORD-1")).contains("default::ORD-1");
    }

    @Test
    void aStoredKeyIsNotWidenedToOtherSources() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("shop::ORD-1");

        assertThat(lookup.resolve("partner::ORD-1")).isEmpty();
    }

    @Test
    void aRawIdentifierResolvesToTheOnlyExecutionItCanName() {
        run("default::7f3c-order");

        assertThat(lookup.resolve("7f3c-order")).contains("default::7f3c-order");
    }

    @Test
    void aRawIdentifierResolvesWithinWhicheverSourceHoldsIt() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("partner::ORD-9");

        assertThat(lookup.resolve("ORD-9")).contains("partner::ORD-9");
    }

    @Test
    void anUnknownRawIdentifierResolvesToNothing() {
        run("default::ORD-1");

        assertThat(lookup.resolve("ORD-404")).isEmpty();
    }

    @Test
    void anUnknownStoredKeyResolvesToNothing() {
        assertThat(lookup.resolve("default::ORD-404")).isEmpty();
    }

    @Test
    void nothingAtAllResolvesToNothing() {
        assertThat(lookup.resolve(null)).isEmpty();
        assertThat(lookup.resolve("")).isEmpty();
        assertThat(lookup.resolve("   ")).isEmpty();
    }

    @Test
    void anAliasResolvesToTheExecutionItWasFoldedInto() {
        run("default::ORD-1");
        alias("default", "PAY-7", "default::ORD-1");

        assertThat(lookup.resolve("PAY-7")).contains("default::ORD-1");
        assertThat(lookup.resolve("default::PAY-7")).contains("default::ORD-1");
    }

    @Test
    void anAliasPointingAtNothingIsNotAnAnswer() {
        alias("default", "PAY-7", "default::ORD-GONE");

        assertThat(lookup.resolve("PAY-7")).isEmpty();
    }

    @Test
    void aRawIdentifierNamingTwoSystemsIsRefusedRatherThanPicked() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("shop::ORD-1");
        run("partner::ORD-1");

        assertThatThrownBy(() -> lookup.resolve("ORD-1"))
                .isInstanceOf(ExecutionLookup.AmbiguousExecutionException.class)
                .hasMessageContaining("shop::ORD-1")
                .hasMessageContaining("partner::ORD-1");
    }

    @Test
    void namingTheSourceSettlesAnAmbiguousIdentifier() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("shop::ORD-1");
        run("partner::ORD-1");

        assertThat(lookup.resolve("shop::ORD-1")).contains("shop::ORD-1");
        assertThat(lookup.resolve("partner::ORD-1")).contains("partner::ORD-1");
    }

    @Test
    void twoRoutesToOneExecutionAreNotAmbiguous() {
        sourceNames.add("shop");
        run("shop::ORD-1");
        alias("default", "ORD-1", "shop::ORD-1");

        assertThat(lookup.resolve("ORD-1")).contains("shop::ORD-1");
    }

    @Test
    void aSingleColonInsideAnIdentifierIsPartOfIt() {
        run("default::urn:order:1");

        assertThat(lookup.resolve("urn:order:1")).contains("default::urn:order:1");
    }

    private void run(String executionId) {
        SagaInstance instance = new SagaInstance();
        instance.setSagaId(executionId);
        instance.setSourceName(ExecutionKey.parse(executionId).source());
        instance.setStartTime(T0);
        instance.setLastUpdated(T0);
        instances.put(executionId, instance);
    }

    private void alias(String source, String rawId, String executionId) {
        aliasRows.put(new ExecutionAlias.Key(source, rawId),
                new ExecutionAlias(source, rawId, executionId, "e1", T0));
    }
}
