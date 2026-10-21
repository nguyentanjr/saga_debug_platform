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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Which execution an observation belongs to, when a system does not have just one id.
 *
 * <h2>The assumption this removes</h2>
 * Every observation had to arrive carrying the same identifier as every other observation
 * of its execution. Systems designed around a saga orchestrator do that. Systems that were
 * not — the ones actually worth attaching a debugger to — do not: the order service knows
 * an order number, the payment service knows a payment id, the shipping service knows a
 * consignment reference, and each publishes the one it knows.
 *
 * <p>Those runs used to split into several executions, each looking half-finished, and the
 * platform reported a stuck saga per fragment. The only remedy on offer was to ask the
 * observed system to propagate a shared id — a change to code this platform is supposed to
 * observe rather than modify.
 *
 * <h2>What a join is made of, and what it is never made of</h2>
 * One thing: an observation that carried two identifiers at once, because the producer put
 * both in the same event. That is an observed fact with an event id behind it, in the same
 * sense as {@code causationId}.
 *
 * <p>Never: a shared business key, a shared event type, timestamps that fall close
 * together, or two identifiers that happen to be the same string. A busy system produces
 * all four constantly. A wrong link here would need a producer to have emitted a wrong
 * pair.
 *
 * <h2>Aliases are scoped to their source</h2>
 * Both sides of a link are {@link ExecutionKey}s, so {@code PAY-9} in one observed system
 * and {@code PAY-9} in another are different keys and cannot resolve to each other. A link
 * that deliberately crosses systems is possible — a mapping declares which namespace its
 * alias belongs to — but it is then an explicit statement in that source's mapping, backed
 * by the event that carried both, rather than a collision the platform fell into.
 */
@Component
public class ExecutionAliasResolver {

    private static final Logger log = LoggerFactory.getLogger(ExecutionAliasResolver.class);

    private final ExecutionAliasRepository aliases;
    private final SagaEventRepository sagaEvents;
    private final SagaInstanceRepository sagaInstances;
    private final AnomalyRepository anomalies;
    private final RcaResultRepository rcaResults;

    public ExecutionAliasResolver(ExecutionAliasRepository aliases,
            SagaEventRepository sagaEvents,
            SagaInstanceRepository sagaInstances,
            AnomalyRepository anomalies,
            RcaResultRepository rcaResults) {
        this.aliases = aliases;
        this.sagaEvents = sagaEvents;
        this.sagaInstances = sagaInstances;
        this.anomalies = anomalies;
        this.rcaResults = rcaResults;
    }

    /**
     * The execution this observation should be filed under.
     *
     * <h2>Joins the caller's transaction</h2>
     * This used to open one of its own, on the reasoning that an alias is a standing fact
     * about two identifiers whether or not the observation that revealed it is stored.
     * That reasoning was wrong in a specific way: an alias row cites the event that carried
     * both identifiers, in {@code linkedByEventId}. A row asserting "these two are one run,
     * see event e3" while e3 was never stored is not a fact, it is a dangling citation —
     * and a merge committed on that basis moved observations, folded instances and withdrew
     * findings on evidence the database does not hold.
     *
     * <p>So identity now commits with the observation that established it, or not at all.
     * Re-registering an existing link is still a no-op, which is what keeps a redelivery
     * from either losing the link or duplicating it.
     *
     * @param declared the source's declared correlation key, already namespaced
     * @param alias    the second identifier this observation carried, namespaced under
     *                 whichever source its mapping says it belongs to, or null
     * @return the key to correlate under: {@code declared} itself unless that execution is
     *         already known by another name
     */
    @Transactional
    public ExecutionKey canonical(ExecutionKey declared, ExecutionKey alias,
            String eventId, Instant observedAt) {
        if (declared == null) {
            return null;
        }
        if (alias == null || alias.equals(declared)) {
            // Nothing new is claimed, but this key may itself already be an alias.
            Optional<ExecutionKey> known = lookup(declared);
            if (known.isPresent()) {
                return known.get();
            }
            // First sighting: the key claims itself (EXT-ALIAS-RACE). Without this row, a
            // worker linking this key from another identifier at the same moment could not
            // see this execution — it is not committed yet — and would file the key under
            // its own run, leaving this one behind for good. With it, the primary key of
            // execution_aliases arbitrates: the second claimant fails on commit, its record
            // is retried, and the retry reads the committed execution and folds it.
            register(declared, declared, eventId, observedAt);
            return declared;
        }

        ExecutionKey left = lookup(declared).orElse(declared);
        ExecutionKey right = lookup(alias).orElse(alias);
        if (left.equals(right)) {
            return left;
        }

        if (!declared.sameSourceAs(alias)) {
            // Reaching here means a mapping said so: the alias key was namespaced under
            // another source deliberately. Worth saying out loud every time, because it is
            // the one path that can fold two observed systems into one execution.
            log.warn("Cross-source link declared: {} and {} are the same run per event {}",
                    declared.canonical(), alias.canonical(), eventId);
        }

        // Whether either key already carries a run decides what has to happen. An alias
        // row is only written when a link is seen, so the alias table alone cannot answer
        // this: an execution that has never been part of a link has no row in it and would
        // look new — which is how the two halves this exists to join were left apart.
        Optional<SagaInstance> leftInstance = sagaInstances.findById(left.canonical());
        Optional<SagaInstance> rightInstance = sagaInstances.findById(right.canonical());

        if (leftInstance.isPresent() && rightInstance.isPresent()) {
            return merge(left, leftInstance, right, rightInstance, eventId, observedAt);
        }

        // At most one side has anything stored, so nothing needs moving. The declared
        // correlation key stays canonical — a source that does have a saga id keeps using
        // it — unless the other key is the one already carrying the run.
        ExecutionKey canonical = rightInstance.isPresent() ? right : left;
        register(left, canonical, eventId, observedAt);
        register(right, canonical, eventId, observedAt);
        return canonical;
    }

    /**
     * Folds two executions that turned out to be one.
     *
     * <p>The survivor is the one observed first. That is a deterministic rule rather than a
     * judgement about which chain matters more: two workers reaching this from opposite
     * directions must choose the same survivor, or each folds the other and the execution
     * ends up in two places again.
     *
     * <p>Everything filed under the loser is re-pointed rather than copied: observations,
     * anomalies already raised against the fragment, and any diagnosis produced while the
     * run looked incomplete. The findings stay, and stay attached to the events that caused
     * them — a merge must not quietly erase evidence — but they now hang off the whole
     * execution, where the next detector pass reads them against the full sequence.
     */
    ExecutionKey merge(ExecutionKey left, Optional<SagaInstance> leftInstance,
            ExecutionKey right, Optional<SagaInstance> rightInstance,
            String eventId, Instant observedAt) {

        // Both rows are locked before anything moves, in an order derived from the keys
        // themselves. Two workers that reach the same pair from opposite sides therefore
        // queue rather than interleave, and they cannot deadlock by taking the pair in
        // opposite orders. Without this the re-pointing below is a bulk UPDATE that the
        // @Version column on SagaInstance cannot see: a concurrent writer holding the
        // losing instance would overwrite the fold, or resurrect a row this merge deleted.
        lockInOrder(left, right);

        // Re-read under the lock. Waiting for it may have meant waiting for another worker
        // to perform this very merge, in which case there is nothing left to do and doing
        // it again would move rows that already moved.
        ExecutionKey settledLeft = lookup(left).orElse(left);
        ExecutionKey settledRight = lookup(right).orElse(right);
        if (settledLeft.equals(settledRight)) {
            log.debug("Executions {} and {} were already folded while this worker waited",
                    left.canonical(), right.canonical());
            return settledLeft;
        }

        leftInstance = sagaInstances.findById(settledLeft.canonical());
        rightInstance = sagaInstances.findById(settledRight.canonical());
        left = settledLeft;
        right = settledRight;

        ExecutionKey canonical = survivorOf(left, leftInstance, right, rightInstance);
        ExecutionKey merged = canonical.equals(left) ? right : left;

        log.info("Executions {} and {} are one run: event {} carried both identifiers. "
                + "Filing everything under {}.",
                left.canonical(), right.canonical(), eventId, canonical.canonical());

        int movedEvents = sagaEvents.repointSaga(merged.canonical(), canonical.canonical());
        int movedAnomalies = anomalies.repointSaga(merged.canonical(), canonical.canonical());
        int movedDiagnoses = rcaResults.repointSaga(merged.canonical(), canonical.canonical());

        // Every finding of the form "X was never observed" was made against half the
        // evidence. The merge has just supplied the other half, so those claims no longer
        // rest on anything and are withdrawn rather than deleted or left standing. The
        // scheduled detectors raise them again on their next pass if they are still true
        // of the whole execution, which is what makes withdrawing them safe.
        int retracted = anomalies.retractClaims(canonical.canonical(), AnomalyType.absenceClaims(),
                Instant.now(),
                "executions " + left.canonical() + " and " + right.canonical()
                        + " were folded into one by event " + eventId
                        + "; this finding was made against a fragment");

        Optional<SagaInstance> survivor = canonical.equals(left) ? leftInstance : rightInstance;
        Optional<SagaInstance> absorbed = canonical.equals(left) ? rightInstance : leftInstance;
        survivor.ifPresent(instance -> absorbed.ifPresent(other -> fold(instance, other)));
        absorbed.ifPresent(sagaInstances::delete);

        // Every name the absorbed execution answered to now resolves to the survivor, so an
        // observation arriving later under any of them lands in the right place.
        for (ExecutionAlias alias : aliases.findByExecutionId(merged.canonical())) {
            alias.setExecutionId(canonical.canonical());
            aliases.save(alias);
        }
        register(merged, canonical, eventId, observedAt);
        register(canonical, canonical, eventId, observedAt);

        log.info("Merged {} into {}: {} observation(s), {} anomaly/anomalies, {} diagnosis/es, "
                        + "{} absence claim(s) withdrawn",
                merged.canonical(), canonical.canonical(),
                movedEvents, movedAnomalies, movedDiagnoses, retracted);
        return canonical;
    }

    /** The execution observed first, falling back to a stable comparison of the keys. */
    private static ExecutionKey survivorOf(ExecutionKey left, Optional<SagaInstance> leftInstance,
            ExecutionKey right, Optional<SagaInstance> rightInstance) {
        Instant leftStart = leftInstance.map(SagaInstance::getStartTime).orElse(null);
        Instant rightStart = rightInstance.map(SagaInstance::getStartTime).orElse(null);

        if (leftStart != null && rightStart != null) {
            if (leftStart.isBefore(rightStart)) return left;
            if (rightStart.isBefore(leftStart)) return right;
        } else if (leftStart != null) {
            return left;
        } else if (rightStart != null) {
            return right;
        }

        // Same instant, or neither has an instance yet. Ordering by the key keeps two
        // concurrent workers from choosing differently and merging in both directions.
        return left.canonical().compareTo(right.canonical()) <= 0 ? left : right;
    }

    /**
     * Widens the survivor to cover the run both halves describe.
     *
     * <p>Unless the workflow is disputed, status is left alone. The observation that revealed the link is still
     * being processed, and the correlation service derives status from the workflow
     * specification once it sees the whole sequence — a status invented here would be this
     * class guessing at something the specification answers.
     */
    private void fold(SagaInstance survivor, SagaInstance absorbed) {
        if (absorbed.getStartTime() != null
                && (survivor.getStartTime() == null
                    || absorbed.getStartTime().isBefore(survivor.getStartTime()))) {
            survivor.setStartTime(absorbed.getStartTime());
        }
        if (absorbed.getLastUpdated() != null
                && (survivor.getLastUpdated() == null
                    || absorbed.getLastUpdated().isAfter(survivor.getLastUpdated()))) {
            survivor.setLastUpdated(absorbed.getLastUpdated());
        }
        if (survivor.getEndTime() == null) {
            survivor.setEndTime(absorbed.getEndTime());
        }
        if (survivor.getOrderId() == null) {
            survivor.setOrderId(absorbed.getOrderId());
        }
        // A half that recognised its workflow tells the merged run which one it is; the
        // other half may never have carried a recognisable start event.
        if (SagaInstance.UNRESOLVED.equals(survivor.getWorkflowType())
                && !SagaInstance.UNRESOLVED.equals(absorbed.getWorkflowType())) {
            survivor.setWorkflowType(absorbed.getWorkflowType());
        }
        survivor.recordWorkflowConflict(absorbed.getWorkflowType());
        absorbed.getWorkflowConflicts().forEach(survivor::recordWorkflowConflict);
        if (survivor.hasWorkflowConflict()) {
            // The linking observation can be a replay and skip projection entirely.
            // Invalidate the old outcome now, inside the same merge transaction.
            survivor.setStatus(SagaStatus.IN_PROGRESS);
            survivor.setEndTime(null);
        }
        sagaInstances.save(survivor);
    }

    /**
     * Takes the row locks a fold needs, in a total order.
     *
     * <p>The order is the keys' own string order, so it is the same for every worker
     * regardless of which side of the pair each one arrived from. That is what makes two
     * concurrent merges of the same pair safe: one waits, and finds the work done.
     */
    private void lockInOrder(ExecutionKey a, ExecutionKey b) {
        Stream.of(a.canonical(), b.canonical())
                .sorted()
                .forEach(sagaInstances::findByIdForUpdate);
    }

    private Optional<ExecutionKey> lookup(ExecutionKey key) {
        return aliases.findById(ExecutionAlias.Key.of(key))
                .map(ExecutionAlias::getExecutionId)
                .map(ExecutionKey::parse);
    }

    private void register(ExecutionKey aliasKey, ExecutionKey canonical,
            String eventId, Instant observedAt) {
        Optional<ExecutionAlias> existing = aliases.findById(ExecutionAlias.Key.of(aliasKey));
        if (existing.isPresent()) {
            ExecutionAlias alias = existing.get();
            if (!alias.getExecutionId().equals(canonical.canonical())) {
                alias.setExecutionId(canonical.canonical());
                aliases.save(alias);
            }
            return;
        }
        aliases.save(new ExecutionAlias(aliasKey.source(), aliasKey.rawId(),
                canonical.canonical(), eventId,
                observedAt != null ? observedAt : Instant.now()));
    }

    /** Every identifier this execution is known by, for a report that has to explain itself. */
    public List<String> identifiersOf(ExecutionKey executionKey) {
        return aliases.findByExecutionId(executionKey.canonical()).stream()
                .map(alias -> ExecutionKey.of(alias.getSourceName(), alias.getAliasKey())
                        .canonical())
                .toList();
    }
}
