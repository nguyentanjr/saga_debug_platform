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

/**
 * Correlating a system whose services do not share one identifier.
 *
 * <p>This is the assumption that decided which systems could be observed at all. A platform
 * that requires one id running through every service can only be attached to a system that
 * already has a saga orchestrator — which is to say, to a system that already knows what
 * its distributed transactions are. The ones where a debugger earns its keep are the ones
 * that grew without that: the order service knows an order number, the payment service
 * knows a payment id, and one event mentions both.
 *
 * <p>Every key here is an {@link ExecutionKey}, so the tests below also pin down what a
 * link is <em>not</em> allowed to do: reach across observed systems on the strength of a
 * shared string.
 */
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

    // ── a source that declares no second identifier ───────────────────────────

    /**
     * The overwhelming majority of sources. They must pay nothing for a feature they do not
     * use — not a row, not a lookup that turns into a write.
     */
    @Test
    void aSourceWithNoAliasKeyIsUnaffected() {
        assertThat(resolver.canonical(key(SHOP, "saga-1"), null, "e1", T0))
                .isEqualTo(key(SHOP, "saga-1"));

        // The key claims itself and nothing else (EXT-ALIAS-RACE): no link to another id.
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

    // ── the ordinary case: the link arrives with the first event ──────────────

    /**
     * The order service publishes {@code orderId} and mentions the payment it created. From
     * then on the payment service's own events, which know nothing about orders, still land
     * in the right execution.
     */
    @Test
    void anEventCarryingBothIdentifiersTiesThemTogether() {
        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0))
                .isEqualTo(key(SHOP, "ORD-1"));

        // The payment service publishes only what it knows.
        assertThat(resolver.canonical(key(SHOP, "PAY-9"), null, "e2", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    /** Whichever of the two a later event arrives under, it resolves the same way. */
    @Test
    void eitherIdentifierResolvesToTheSameExecution() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);

        assertThat(resolver.canonical(key(SHOP, "ORD-1"), null, "e2", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
        assertThat(resolver.canonical(key(SHOP, "PAY-9"), null, "e3", T0.plusSeconds(2)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    /** The link is evidence, and evidence names the observation it came from. */
    @Test
    void theLinkRecordsTheEventThatRevealedIt() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);

        ExecutionAlias link = stored.get(new ExecutionAlias.Key(SHOP, "PAY-9"));
        assertThat(link.getLinkedByEventId()).isEqualTo("e1");
        assertThat(link.getObservedAt()).isEqualTo(T0);
        assertThat(link.getExecutionId()).isEqualTo("shop::ORD-1");
    }

    /**
     * A third identifier joins the same execution rather than starting a chain of its own,
     * so a saga crossing four services is still one execution and not four.
     */
    @Test
    void aThirdIdentifierJoinsTheSameExecution() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);
        resolver.canonical(key(SHOP, "PAY-9"), key(SHOP, "SHIP-3"), "e2", T0.plusSeconds(1));

        assertThat(resolver.canonical(key(SHOP, "SHIP-3"), null, "e3", T0.plusSeconds(2)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    // ── the same string, two observed systems ─────────────────────────────────

    /**
     * The defect this scoping exists for. Two systems both use {@code PAY-9} as an alias,
     * and neither run has anything to do with the other.
     *
     * <p>Keyed on the bare identifier, the second registration would overwrite the first
     * and every later payment observation of one system would resolve into the other
     * system's execution.
     */
    @Test
    void twoSystemsUsingTheSameAliasDoNotResolveToEachOther() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);
        resolver.canonical(key(PARTNER, "REF-77"), key(PARTNER, "PAY-9"), "e2", T0);

        assertThat(resolver.canonical(key(SHOP, "PAY-9"), null, "e3", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
        assertThat(resolver.canonical(key(PARTNER, "PAY-9"), null, "e4", T0.plusSeconds(1)))
                .isEqualTo(key(PARTNER, "REF-77"));
    }

    /**
     * Two executions bearing the same raw identifier in different systems are never folded,
     * however much they look alike. Nothing links them but a string.
     */
    @Test
    void twoSystemsUsingTheSameExecutionIdAreNeverMerged() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(PARTNER, "ORD-1", T0.plusSeconds(5), SagaStatus.IN_PROGRESS);

        // An observation of the partner's run, carrying the partner's own second id.
        resolver.canonical(key(PARTNER, "ORD-1"), key(PARTNER, "PAY-2"), "e1", T0.plusSeconds(6));

        assertThat(instances).containsKeys("shop::ORD-1", "partner::ORD-1");
        verify(sagaEvents, never()).repointSaga(anyString(), anyString());
    }

    /**
     * A link that does cross systems is possible, but only because a mapping said so — and
     * it is loud when it happens. This is the gateway case: one system publishes both its
     * own reference and the partner's, in the same event.
     */
    @Test
    void aDeclaredCrossSourceLinkIsHonouredBecauseAnEventCarriedBothIdentifiers() {
        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(PARTNER, "REF-77"), "e1", T0))
                .isEqualTo(key(SHOP, "ORD-1"));

        assertThat(resolver.canonical(key(PARTNER, "REF-77"), null, "e2", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    // ── the hard case: the link arrives after both halves exist ───────────────

    /**
     * Nothing guarantees the linking event is observed first. Payment events can be read
     * before whatever announced which order the payment belongs to, so by the time the link
     * appears both identifiers already have a run of their own.
     */
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

    /**
     * Findings raised while the run looked like a fragment are re-pointed, not deleted. A
     * merge that quietly dropped evidence would be the platform hiding what it had already
     * reported.
     */
    @Test
    void aMergeMovesFindingsRatherThanDiscardingThem() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        verify(anomalies).repointSaga("shop::PAY-9", "shop::ORD-1");
        verify(anomalies, never()).deleteAll();
    }

    /**
     * A claim of absence made about a fragment is withdrawn, not left standing.
     *
     * <p>{@code ORPHAN_EVENT} says "no event that starts a workflow was seen before this
     * one". That was true of the half it was raised against and false of the whole run the
     * merge just assembled. Leaving it means the platform reports something it now knows to
     * be untrue; deleting it means a debugging platform quietly removes findings it has
     * already shown someone. It is withdrawn, with the reason, and the detectors are free
     * to raise it again if it still holds.
     */
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

    /**
     * The merge log reports the real number of withdrawn claims.
     *
     * <p>Regression. The log statement gained a placeholder for the count without gaining
     * the argument to fill it, so every merge printed a literal <code>{}</code> where the
     * number belonged. Nothing failed and no test noticed: the retraction itself worked,
     * only the operator's account of it was wrong. It was found by reading the log during
     * live verification, which is not a mechanism to rely on.
     *
     * <p>Asserting on a log line is unusual and deliberate here. This line is the only
     * place a merge reports what it did to findings already shown to someone, so its
     * accuracy is the behaviour, not decoration.
     */
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
        // The shape of the original defect: a placeholder with no argument behind it.
        assertThat(merged).doesNotContain("{}");
    }

    /**
     * A redelivery of the linking event does not withdraw anything a second time. The
     * count an operator reads is the number of claims this merge actually falsified, not
     * a tally that grows with every retry.
     */
    @Test
    void aRedeliveredLinkDoesNotWithdrawClaimsAgain() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        // The second pass finds both keys already resolving to the same execution and
        // stops before the fold, so nothing is re-pointed and nothing is re-withdrawn.
        verify(anomalies, times(1)).retractClaims(anyString(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(Instant.class), anyString());
        verify(sagaEvents, times(1)).repointSaga(anyString(), anyString());
    }

    /**
     * Only claims of absence. An event that arrived after a terminal state really did
     * arrive; observing more of the run cannot make that untrue, and withdrawing it would
     * be the platform erasing a finding it still stands behind.
     */
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

    /**
     * Every type declares the shape of its own claim, so a detector added later is covered
     * without this merge code being taught about it.
     */
    @Test
    void everyAnomalyTypeSaysWhichKindOfClaimItMakes() {
        assertThat(AnomalyType.ORPHAN_EVENT.assertsAbsence()).isTrue();
        assertThat(AnomalyType.MISSING_TERMINAL_EVENT.assertsAbsence()).isTrue();
        assertThat(AnomalyType.COMPENSATION_MISSING.assertsAbsence()).isTrue();
        assertThat(AnomalyType.FORBIDDEN_POST_TERMINAL.assertsAbsence()).isFalse();
        assertThat(AnomalyType.STEP_TIMEOUT.assertsAbsence()).isFalse();
    }

    /**
     * Two workers folding the same pair must not both fold it.
     *
     * <p>The rows are locked in an order derived from the keys, so whichever worker arrives
     * second waits. What it must then do is re-read: waiting for the lock may have meant
     * waiting for exactly this merge to be performed by someone else, and repeating it
     * would move rows that already moved and withdraw findings twice.
     *
     * <p>Simulated by having the lock acquisition itself complete the other worker's merge,
     * which is what the waiting worker would have observed.
     */
    @Test
    void aWorkerThatWaitedForTheLockFindsTheMergeAlreadyDone() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);

        // While this worker blocks on the row lock, the other worker commits the fold.
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
        // Nothing moved a second time.
        verify(sagaEvents, never()).repointSaga(anyString(), anyString());
        verify(anomalies, never()).repointSaga(anyString(), anyString());
    }

    /**
     * The rows are locked before anything is moved, and in an order that does not depend on
     * which side of the pair this worker arrived from — otherwise two workers approaching
     * the same pair from opposite directions deadlock.
     */
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

    /** The merged run covers the whole span, not just the survivor's half. */
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

    /**
     * Only one half needs to have carried a recognisable start event. The other may have
     * begun mid-saga, from a service that publishes nothing a specification declares as a
     * beginning — which is exactly why it was a fragment.
     */
    @Test
    void theMergedExecutionKeepsTheWorkflowEitherHalfRecognised() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS)
                .setWorkflowType(SagaInstance.UNRESOLVED);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS)
                .setWorkflowType("ORDER_SAGA");

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e3", T0.plusSeconds(60));

        assertThat(instances.get("shop::ORD-1").getWorkflowType()).isEqualTo("ORDER_SAGA");
    }

    /**
     * Two workers merging the same pair from opposite directions must agree, or each folds
     * the other's survivor and the execution ends up in two places again. Start times
     * normally settle it; when they are identical the keys themselves do, because something
     * has to and it cannot be the order the events happened to arrive.
     */
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

    /**
     * The declared correlation key stays canonical when nothing has been stored under
     * either identifier yet. Nothing has to move in that case, so there is no reason to
     * file the run under the secondary name a producer happened to mention.
     */
    @Test
    void theDeclaredCorrelationKeyIsTheCanonicalOneWhenNeitherSideHasRunYet() {
        assertThat(resolver.canonical(key(SHOP, "ZZZ-order"), key(SHOP, "AAA-payment"), "e1", T0))
                .isEqualTo(key(SHOP, "ZZZ-order"));
    }

    /**
     * Unless the other identifier is the one already carrying the run. Filing the new
     * observation under a fresh key would leave the existing half stranded, which is the
     * exact failure this class exists to prevent.
     */
    @Test
    void anIdentifierThatAlreadyCarriesTheRunWinsOverAFreshOne() {
        givenInstance(SHOP, "PAY-9", T0, SagaStatus.IN_PROGRESS);

        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0.plusSeconds(1)))
                .isEqualTo(key(SHOP, "PAY-9"));
    }

    /** After a merge, every name the absorbed run answered to points at the survivor. */
    @Test
    void everyIdentifierOfTheAbsorbedRunFollowsIt() {
        givenInstance(SHOP, "ORD-1", T0, SagaStatus.IN_PROGRESS);
        givenInstance(SHOP, "PAY-9", T0.plusSeconds(30), SagaStatus.IN_PROGRESS);
        resolver.canonical(key(SHOP, "PAY-9"), key(SHOP, "REFUND-2"), "e1", T0.plusSeconds(30));

        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e2", T0.plusSeconds(60));

        assertThat(resolver.canonical(key(SHOP, "REFUND-2"), null, "e3", T0.plusSeconds(70)))
                .isEqualTo(key(SHOP, "ORD-1"));
    }

    /** Re-seeing a link that is already recorded changes nothing and moves nothing. */
    @Test
    void seeingTheSameLinkAgainIsANoOp() {
        resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1", T0);

        assertThat(resolver.canonical(key(SHOP, "ORD-1"), key(SHOP, "PAY-9"), "e1-redelivered", T0))
                .isEqualTo(key(SHOP, "ORD-1"));
        verify(sagaEvents, never()).repointSaga(anyString(), anyString());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

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
