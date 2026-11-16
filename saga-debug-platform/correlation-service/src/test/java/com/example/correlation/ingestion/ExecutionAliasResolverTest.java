package com.example.correlation.ingestion;

import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.ExecutionAlias;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExecutionAliasResolverTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SHOP = "shop";
    private static final String PARTNER = "partner";

    private Map<ExecutionAlias.Key, ExecutionAlias> stored;
    private Map<String, SagaInstance> instances;

    private ExecutionAliasRepository aliases;
    private SagaEventRepository sagaEvents;
    private SagaInstanceRepository sagaInstances;
    private AnomalyRepository anomalies;
    private RcaResultRepository rcaResults;
    private ExecutionAliasResolver resolver;

    @BeforeEach
    void setUp() {
        stored = new LinkedHashMap<>();
        instances = new LinkedHashMap<>();

        aliases = mock(ExecutionAliasRepository.class);
        when(aliases.findById(any(ExecutionAlias.Key.class)))
                .thenAnswer(call -> Optional.ofNullable(stored.get(call.getArgument(0))));
        when(aliases.save(any(ExecutionAlias.class))).thenAnswer(call -> {
            ExecutionAlias alias = call.getArgument(0);
            stored.put(new ExecutionAlias.Key(alias.getSourceName(), alias.getAliasKey()), alias);
            return alias;
        });
        when(aliases.findByExecutionId(anyString())).thenAnswer(call -> {
            String executionId = call.getArgument(0);
            List<ExecutionAlias> found = new ArrayList<>();
            stored.values().forEach(alias -> {
                if (alias.getExecutionId().equals(executionId)) found.add(alias);
            });
            return found;
        });

        sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findById(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstances.save(any(SagaInstance.class))).thenAnswer(call -> {
            SagaInstance instance = call.getArgument(0);
            instances.put(instance.getSagaId(), instance);
            return instance;
        });
        org.mockito.Mockito.doAnswer(call -> {
            instances.remove(((SagaInstance) call.getArgument(0)).getSagaId());
            return null;
        }).when(sagaInstances).delete(any(SagaInstance.class));

        sagaEvents = mock(SagaEventRepository.class);
        anomalies = mock(AnomalyRepository.class);
        rcaResults = mock(RcaResultRepository.class);

        resolver = new ExecutionAliasResolver(
                aliases, sagaEvents, sagaInstances, anomalies, rcaResults);
    }

    @Test
    void aSourceWithNoAliasKeyIsUnaffected() {
        assertThat(resolver.canonical(key(SHOP, "saga-1"), null, "e1", T0))
                .isEqualTo(key(SHOP, "saga-1"));

        assertThat(stored.values()).extracting(ExecutionAlias::getExecutionId)
                .containsExactly(key(SHOP, "saga-1").canonical());
    }

    @Test
    void anAliasIdenticalToTheCorrelationKeyClaimsNothing() {
        assertThat(resolver.canonical(key(SHOP, "saga-1"), key(SHOP, "saga-1"), "e1", T0))
                .isEqualTo(key(SHOP, "saga-1"));

        assertThat(stored.values()).extracting(ExecutionAlias::getExecutionId)
                .containsExactly(key(SHOP, "saga-1").canonical());
    }

    @Test
    void anEventCarryingBothIdentifiersTiesThemTogether() {
        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0))
                .isEqualTo(key(SHOP, "ORD-1"));

        assertThat(resolver.canonical(key(SHOP, "PAY-9"), null, "e2", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    @Test
    void eitherIdentifierResolvesToTheSameExecution() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);

        assertThat(resolver.canonical(key(SHOP, "ORD-1"), null, "e2", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
        assertThat(resolver.canonical(key(SHOP, "PAY-9"), null, "e3", T0.plusSeconds(2)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    @Test
    void theLinkRecordsTheEventThatRevealedIt() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);

        ExecutionAlias link = stored.get(new ExecutionAlias.Key(SHOP, "PAY-9"));
        assertThat(link.getLinkedByEventId()).isEqualTo("e1");
        assertThat(link.getObservedAt()).isEqualTo(T0);
        assertThat(link.getExecutionId()).isEqualTo("shop::ORD-1");
    }

    @Test
    void aThirdIdentifierJoinsTheSameExecution() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);
        resolver.canonical(key(SHOP, "PAY-9"), key(SHOP, "SHIP-3"), "e2", T0.plusSeconds(1));

        assertThat(resolver.canonical(key(SHOP, "SHIP-3"), null, "e3", T0.plusSeconds(2)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    @Test
    void twoSystemsUsingTheSameAliasDoNotResolveToEachOther() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);
        resolver.canonical(key(PARTNER, "REF-77"), key(PARTNER, "PAY-9"), "e2", T0);

        assertThat(resolver.canonical(key(SHOP, "PAY-9"), null, "e3", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
        assertThat(resolver.canonical(key(PARTNER, "PAY-9"), null, "e4", T0.plusSeconds(1)))
                .isEqualTo(key(PARTNER, "REF-77"));
    }

    @Test
    void twoSystemsUsingTheSameExecutionIdAreNeverMerged() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(PARTNER, "ORD-1", T0.plusSeconds(5), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(PARTNER, "ORD-1"), key(PARTNER, "PAY-2"), "e1", T0.plusSeconds(6));

        assertThat(instances).containsKeys("shop::ORD-1", "partner::ORD-1");
        verify(sagaEvents, never()).repointSaga(anyString(), anyString());
    }

    @Test
    void aDeclaredCrossSourceLinkIsHonouredBecauseAnEventCarriedBothIdentifiers() {
        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(PARTNER, "REF-77"), "e1", T0))
                .isEqualTo(key(SHOP, "ORD-1"));

        assertThat(resolver.canonical(key(PARTNER, "REF-77"), null, "e2", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    @Test
    void twoExecutionsBuiltIndependentlyAreFoldedIntoOne() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        ExecutionKey canonical = resolver.canonical(
                key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        assertThat(canonical).isEqualTo(key(SHOP, "ORD-1"));
        verify(sagaEvents).repointSaga("shop::PAY-9", "shop::ORD-1");
        verify(anomalies).repointSaga("shop::PAY-9", "shop::ORD-1");
        verify(rcaResults).repointSaga("shop::PAY-9", "shop::ORD-1");
        assertThat(instances).doesNotContainKey("shop::PAY-9");
    }

    @Test
    void aMergeMovesFindingsRatherThanDiscardingThem() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        verify(anomalies).repointSaga("shop::PAY-9", "shop::ORD-1");
        verify(anomalies, never()).deleteAll();
    }

    @Test
    void aMergeWithdrawsClaimsOfAbsenceItHasJustFalsified() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        ArgumentCaptor<java.util.Collection<AnomalyType>> types =
                ArgumentCaptor.forClass(java.util.Collection.class);
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(anomalies).retractClaims(
                org.mockito.ArgumentMatchers.eq("shop::ORD-1"),
                types.capture(),
                org.mockito.ArgumentMatchers.any(Instant.class),
                reason.capture());

        assertThat(types.getValue()).contains(AnomalyType.ORPHAN_EVENT);
        assertThat(reason.getValue()).contains("e3").contains("fragment");
    }

    @Test
    void theMergeLogReportsHowManyClaimsWereActuallyWithdrawn() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);
        when(anomalies.retractClaims(anyString(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(Instant.class), anyString())).thenReturn(3);

        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                        .getLogger(ExecutionAliasResolver.class);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> log =
                new ch.qos.logback.core.read.ListAppender<>();
        log.start();
        logger.addAppender(log);
        try {
            resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));
        } finally {
            logger.detachAppender(log);
        }

        String merged = log.list.stream()
                .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
                .filter(line -> line.startsWith("Merged "))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the merge reported nothing"));

        assertThat(merged).contains("3 absence claim(s) withdrawn");

        assertThat(merged).doesNotContain("{}");
    }

    @Test
    void aRedeliveredLinkDoesNotWithdrawClaimsAgain() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        verify(anomalies, times(1)).retractClaims(anyString(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(Instant.class), anyString());
        verify(sagaEvents, times(1)).repointSaga(anyString(), anyString());
    }

    @Test
    void aMergeLeavesClaimsAboutWhatActuallyHappenedAlone() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        ArgumentCaptor<java.util.Collection<AnomalyType>> types =
                ArgumentCaptor.forClass(java.util.Collection.class);
        verify(anomalies).retractClaims(org.mockito.ArgumentMatchers.anyString(),
                types.capture(), org.mockito.ArgumentMatchers.any(Instant.class),
                org.mockito.ArgumentMatchers.anyString());

        assertThat(types.getValue())
                .doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL)
                .doesNotContain(AnomalyType.OUT_OF_ORDER)
                .doesNotContain(AnomalyType.STATE_REGRESSION)
                .doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    @Test
    void everyAnomalyTypeSaysWhichKindOfClaimItMakes() {
        assertThat(AnomalyType.ORPHAN_EVENT.assertsAbsence()).isTrue();
        assertThat(AnomalyType.MISSING_TERMINAL_EVENT.assertsAbsence()).isTrue();
        assertThat(AnomalyType.COMPENSATION_MISSING.assertsAbsence()).isTrue();
        assertThat(AnomalyType.FORBIDDEN_POST_TERMINAL.assertsAbsence()).isFalse();
        assertThat(AnomalyType.STEP_TIMEOUT.assertsAbsence()).isFalse();
    }

    @Test
    void aWorkerThatWaitedForTheLockFindsTheMergeAlreadyDone() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        when(sagaInstances.findByIdForUpdate(anyString())).thenAnswer(call -> {
            stored.put(new ExecutionAlias.Key(SHOP, "PAY-9"),
                    new ExecutionAlias(SHOP, "PAY-9", "shop::ORD-1", "other-worker-event", T0));
            stored.put(new ExecutionAlias.Key(SHOP, "ORD-1"),
                    new ExecutionAlias(SHOP, "ORD-1", "shop::ORD-1", "other-worker-event", T0));
            instances.remove("shop::PAY-9");
            return Optional.ofNullable(instances.get(call.getArgument(0)));
        });

        ExecutionKey result = resolver.canonical(
                key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        assertThat(result).isEqualTo(key(SHOP, "ORD-1"));

        verify(sagaEvents, never()).repointSaga(anyString(), anyString());
        verify(anomalies, never()).repointSaga(anyString(), anyString());
    }

    @Test
    void bothRowsAreLockedInTheSameOrderWhicheverSideArrivesFirst() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));
        InOrder fromLeft = inOrder(sagaInstances);
        fromLeft.verify(sagaInstances).findByIdForUpdate("shop::ORD-1");
        fromLeft.verify(sagaInstances).findByIdForUpdate("shop::PAY-9");

        setUp();
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "PAY-9"), key(SHOP, "ORD-1"), "e4", T0.plusSeconds(60));
        InOrder fromRight = inOrder(sagaInstances);
        fromRight.verify(sagaInstances).findByIdForUpdate("shop::ORD-1");
        fromRight.verify(sagaInstances).findByIdForUpdate("shop::PAY-9");
    }

    @Test
    void theMergedExecutionSpansBothHalves() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS)
                .setLastUpdated(T0.plusSeconds(90));

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        SagaInstance merged = instances.get("shop::ORD-1");
        assertThat(merged.getStartTime()).isEqualTo(T0);
        assertThat(merged.getLastUpdated()).isEqualTo(T0.plusSeconds(90));
    }

    @Test
    void theMergedExecutionKeepsTheWorkflowEitherHalfRecognised() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS)
                .setWorkflowType(SagaInstance.UNRESOLVED);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS)
                .setWorkflowType("ORDER_SAGA");

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        assertThat(instances.get("shop::ORD-1").getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

    @Test
    void theSurvivorDoesNotDependOnWhichSideTheLinkArrivedFrom() {
        givenInstance(SHOP, "AAA", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "ZZZ", T0, SagaStatus.IN_PROGRESS);
        ExecutionKey fromLeft = resolver.canonical(
                key(SHOP, "AAA"), key(SHOP, "ZZZ"), "e3", T0.plusSeconds(1));

        setUp();
        givenInstance(SHOP, "AAA", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "ZZZ", T0, SagaStatus.IN_PROGRESS);
        ExecutionKey fromRight = resolver.canonical(
                key(SHOP, "ZZZ"), key(SHOP, "AAA"), "e3", T0.plusSeconds(1));

        assertThat(fromLeft).isEqualTo(fromRight);
    }

    @Test
    void theDeclaredCorrelationKeyIsTheCanonicalOneWhenNeitherSideHasRunYet() {
        assertThat(resolver.canonical(key(SHOP, "ZZZ-order"), key(SHOP, "AAA-payment"), "e1", T0))
                .isEqualTo(key(SHOP, "ZZZ-order"));
    }

    @Test
    void anIdentifierThatAlreadyCarriesTheRunWinsOverAFreshOne() {
        givenInstance(SHOP, "PAY-9", T0, SagaStatus.IN_PROGRESS);

        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "PAY-9"));
    }

    @Test
    void everyIdentifierOfTheAbsorbedRunFollowsIt() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);
        resolver.canonical(key(SHOP, "PAY-9"), key(SHOP, "REFUND-2"), "e1", T0.plusSeconds(30));

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e2", T0.plusSeconds(60));

        assertThat(resolver.canonical(key(SHOP, "REFUND-2"), null, "e3", T0.plusSeconds(70)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    @Test
    void seeingTheSameLinkAgainIsANoOp() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);

        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1-redelivered", T0))
                .isEqualTo(key(SHOP, "ORD-1"));
        verify(sagaEvents, never()).repointSaga(anyString(), anyString());
    }

    private static ExecutionKey key(String source, String rawId) {
        return ExecutionKey.of(source, rawId);
    }

    @Test
    void workflowConflictSurvivesAliasMerge() {
        SagaInstance left = givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        SagaInstance right = givenInstance(SHOP, "PAY-9", T0.plusSeconds(1), SagaStatus.IN_PROGRESS);
        left.setWorkflowType("ORDER_SAGA");
        left.setStatus(SagaStatus.COMPLETED);
        left.setEndTime(T0.plusSeconds(1));
        right.setWorkflowType("LOAN_APPROVAL");
        right.recordWorkflowConflict("FULFILLMENT_SAGA");
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "bridge", T0.plusSeconds(2));
        assertThat(left.getWorkflowConflicts())
                .containsExactly("FULFILLMENT_SAGA", "LOAN_APPROVAL", "ORDER_SAGA");
        assertThat(left.getStatus()).isEqualTo(SagaStatus.IN_PROGRESS);
        assertThat(left.getEndTime()).isNull();
    }

    private SagaInstance givenInstance(String source, String rawId, Instant startTime,
            SagaStatus status) {
        ExecutionKey key = key(source, rawId);
        SagaInstance instance = new SagaInstance();
        instance.setSagaId(key.canonical());
        instance.setSourceName(source);
        instance.setStatus(status);
        instance.setStartTime(startTime);
        instance.setLastUpdated(startTime);
        instances.put(key.canonical(), instance);
        return instance;
    }
}
