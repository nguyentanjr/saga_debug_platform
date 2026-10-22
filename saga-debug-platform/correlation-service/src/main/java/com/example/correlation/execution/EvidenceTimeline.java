package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpec.EventRole;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One execution's observations placed in the order the workflow can prove they happened,
 * and the status that whole body of evidence implies.
 *
 * <h2>The two orders</h2>
 * An observer of a distributed system meets two different orders and must never confuse
 * them. <em>Processing order</em> is the order this platform read the events: a property of
 * topics, partitions, consumer threads and retries — of the observer. <em>Business order</em>
 * is the order the work happened: a property of the observed system. Only the second says
 * anything about the run.
 *
 * <p>Status used to be folded in processing order, one event at a time, over the status
 * stored from the last one. Because {@link WorkflowSpec#statusAfter} answers absolutely —
 * {@code INVENTORY_RESERVED} means COMPLETED whatever came before it — folding in
 * processing order makes the answer "whatever the most recently <em>read</em> event
 * implies". A run whose completion was read before its middle step therefore rested at
 * IN_PROGRESS, its {@code endTime} was the instant of whichever event happened to arrive
 * last, and the observer reported {@code STATE_REGRESSION} and
 * {@code FORBIDDEN_POST_TERMINAL} against a system that had done nothing wrong. Every one
 * of those findings described the observer's own read order.
 *
 * <p>So the status is reconstructed from all the evidence in hand, every time, rather than
 * advanced by whichever event arrived. Re-reading the run's events is what makes the answer
 * independent of the order they were read in; there is no cheaper way to be order-invariant
 * than to look at everything. Executions hold a handful of events, and the caller has
 * already loaded them for the detectors.
 *
 * <h2>What decides the order, and what refuses to</h2>
 * Event time first. Where two observations carry the same instant — a producer stamping a
 * burst identically, or a source with no clock of its own falling back to delivery time —
 * the workflow's own dependency graph decides: work at an upstream step happened before
 * work at a downstream one, and within one step a start happened before its completion.
 * That is the workflow proving an order, not the platform assuming one.
 *
 * <p>Where neither says anything — same instant, two steps the graph does not connect —
 * this class does not pick. It reports the contested observations and states no outcome.
 * Sorting still produces some sequence, because a sequence is needed to walk, but nothing
 * is <em>concluded</em> from a pair it cannot order.
 *
 * <h2>Why not {@link ExecutionProjection}</h2>
 * That projection answers a different question — what each step did and how long it took —
 * and to answer it collapses every step to a single operation, discarding the interleaving
 * between steps. Which status-bearing observation is last, and whether the run had already
 * ended when a given event happened, are questions about exactly that interleaving. Adding
 * saga-level status semantics to a step-level timing structure would make the answer to one
 * question depend on the other, so the ordering lives here. Both are built from the same
 * (events, spec) pair and neither touches persistence.
 */
public final class EvidenceTimeline {

    private final WorkflowSpec spec;
    private final List<SagaEvent> ordered;
    private final List<Bearing> bearings;
    private final SagaStatus status;
    private final SagaEvent decidingEvent;
    private final List<SagaEvent> contested;
    private final Regression regression;
    private final Causality causality;
    /** Forward outcomes read while compensation was open: not the run's outcome, nor a handoff. */
    private final Set<SagaEvent> superseded;

    /**
     * An observation that provably followed an ending and reported something lower.
     *
     * <p>Reported only for a provably ordered pair whose earlier half is <em>terminal</em>.
     * A pair whose order is contested cannot evidence a regression: it would be the
     * observer's tie-break showing up as a fault in the observed system, which is the class
     * of bug this file exists to remove. Nor can a pair of non-terminal statuses, for the
     * reasons on {@link #firstRegression}: a running saga moves down the lifecycle ranking
     * legitimately, and whether two such observations even sat next to each other depended
     * on how much of the run had arrived.
     *
     * @param from the ending that was contradicted; always terminal
     */
    public record Regression(SagaEvent earlier, SagaStatus from, SagaEvent later, SagaStatus to) {
    }

    /**
     * An ending, together with an observation the evidence places after it.
     *
     * <p>Reported only for a provably ordered pair, for the reason on {@link Regression}:
     * a pair the evidence does not order would put the observer's own tie-break into a
     * finding against the observed system.
     *
     * @param endedAs the status {@code terminal} established; always terminal
     */
    public record PostTerminal(SagaEvent terminal, SagaStatus endedAs, SagaEvent late) {
    }

    /** An observation together with what it says the run's status is. */
    private record Bearing(SagaEvent event, SagaStatus status) {
    }

    /** What the timeline can prove about the order of two observations. */
    private enum Relation { BEFORE, AFTER, UNDETERMINED }

    private EvidenceTimeline(WorkflowSpec spec, List<SagaEvent> ordered, List<Bearing> bearings,
            SagaStatus status, SagaEvent decidingEvent, List<SagaEvent> contested,
            Regression regression, Causality causality, Set<SagaEvent> superseded) {
        this.causality = causality;
        this.superseded = superseded;
        this.spec = spec;
        this.ordered = ordered;
        this.bearings = bearings;
        this.status = status;
        this.decidingEvent = decidingEvent;
        this.contested = contested;
        this.regression = regression;
    }

    /**
     * What the run's own events say about which caused which (F28).
     *
     * <p>A producer that records the event it was reacting to ({@code causationId}, V9)
     * states an order no clock can contradict: an event cannot happen before its cause.
     * Clocks can, and do. On 2026-10-04 three services stamped a reservation 0.77s before
     * the payment that caused it, and the timeline, ordering by clock, ended the run before
     * it had started. So the clock is read through causality: an event is placed no earlier
     * than its cause ({@link #at}), and a declared ancestor is before its descendants
     * whatever the stamps say ({@link #isAncestor}). Where no causation is declared, or the
     * cause is not in hand, the clock decides exactly as before.
     *
     * <p>EXT-HOTEL-EVENT-TIME: a step's outcome is caused by that step's start, and the
     * specification says so even when the producer does not. Hotel (semotpan, live
     * 2026-10-21) stamped PAYMENT_REJECTED 0.6 s before the TAKE_PAYMENT_REQUESTED it answered;
     * read by clock, the run handed off from payment back to room booking and stood accused of
     * INVALID_TRANSITION. An outcome with no declared cause is therefore read as caused by
     * its step's start -- only when exactly one start of that step is in hand, since with
     * several attempts the specification does not say which one this outcome answers.
     */
    private static final class Causality {
        private final java.util.Map<String, SagaEvent> byId = new java.util.HashMap<>();
        private final java.util.Map<SagaEvent, Instant> at = new java.util.IdentityHashMap<>();
        private final java.util.Map<SagaEvent, Integer> depth = new java.util.IdentityHashMap<>();
        private final java.util.Map<SagaEvent, SagaEvent> stepStart = new java.util.IdentityHashMap<>();

        Causality(WorkflowSpec spec, List<SagaEvent> events) {
            for (SagaEvent e : events) {
                if (e.getEventId() != null) byId.putIfAbsent(e.getEventId(), e);
            }
            if (spec != null) {
                java.util.Map<String, List<SagaEvent>> startsByStep = new java.util.HashMap<>();
                for (SagaEvent e : events) {
                    spec.classifyEvent(e.getEventType())
                            .filter(role -> role.role() == EventRole.Role.STARTED)
                            .ifPresent(role -> startsByStep.computeIfAbsent(role.stepId(), k -> new ArrayList<>()).add(e));
                }
                for (SagaEvent e : events) {
                    spec.classifyEvent(e.getEventType())
                            .filter(role -> role.role() == EventRole.Role.COMPLETED || role.role() == EventRole.Role.FAILED)
                            .map(role -> startsByStep.getOrDefault(role.stepId(), List.of()))
                            .filter(starts -> starts.size() == 1 && starts.get(0) != e)
                            .ifPresent(starts -> stepStart.put(e, starts.get(0)));
                }
            }
            for (SagaEvent e : events) resolve(e, new java.util.HashSet<>());
        }

        private SagaEvent causeOf(SagaEvent e) {
            String id = e.getCausationId();
            if (id != null && !id.equals(e.getEventId())) {
                SagaEvent declared = byId.get(id);
                if (declared != null) return declared;
            }
            return stepStart.get(e);
        }

        private void resolve(SagaEvent e, Set<SagaEvent> visiting) {
            if (at.containsKey(e)) return;
            SagaEvent cause = causeOf(e);
            if (cause == null || !visiting.add(e)) {
                at.put(e, e.getTimestamp());
                depth.put(e, 0);
                return;
            }
            resolve(cause, visiting);
            Instant own = e.getTimestamp();
            Instant causeAt = at.get(cause);
            at.put(e, own == null ? causeAt : causeAt == null || own.isAfter(causeAt) ? own : causeAt);
            depth.put(e, depth.getOrDefault(cause, 0) + 1);
        }

        /** When the event happened, no earlier than anything it was caused by. */
        Instant at(SagaEvent e) {
            return at.containsKey(e) ? at.get(e) : e.getTimestamp();
        }

        int depth(SagaEvent e) {
            return depth.getOrDefault(e, 0);
        }

        /** Whether {@code ancestor} is on {@code event}'s declared chain of causes. */
        boolean isAncestor(SagaEvent ancestor, SagaEvent event) {
            Set<SagaEvent> seen = new java.util.HashSet<>();
            for (SagaEvent cur = causeOf(event); cur != null && seen.add(cur); cur = causeOf(cur)) {
                if (cur == ancestor) return true;
            }
            return false;
        }
    }

    /**
     * @param spec   the workflow this run belongs to, or null while it is still unknown.
     *               Without one nothing can be classified, so the timeline reports only
     *               that the run has been seen — never a guess at where it got to
     * @param events every observation of the run in hand, in any order
     */
    public static EvidenceTimeline of(WorkflowSpec spec, List<SagaEvent> events) {
        List<SagaEvent> given = events == null ? List.of() : events;
        Causality causality = new Causality(spec, given);
        if (spec == null) {
            // A run with no specification has no derivable status. IN_PROGRESS here is not
            // a reading of its evidence; it says only that the run exists and that nothing
            // has been able to judge it.
            return new EvidenceTimeline(null, List.copyOf(given), List.of(),
                    given.isEmpty() ? SagaStatus.STARTED : SagaStatus.IN_PROGRESS,
                    null, List.of(), null, causality, Set.of());
        }

        List<SagaEvent> ordered = new ArrayList<>(given);
        ordered.sort(businessOrder(spec, causality));

        Set<SagaEvent> superseded = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        List<Bearing> bearings = bearingsOf(spec, ordered, superseded);

        if (bearings.isEmpty()) {
            return new EvidenceTimeline(spec, List.copyOf(ordered), List.of(),
                    ordered.isEmpty() ? SagaStatus.STARTED : SagaStatus.IN_PROGRESS,
                    null, List.of(), null, causality, superseded);
        }

        // The run's status is what the last status-bearing observation says — but "last"
        // has to be provable. Every observation the timeline cannot order against that one
        // is an equally good candidate for the last position.
        Bearing last = bearings.get(bearings.size() - 1);
        List<Bearing> contenders = new ArrayList<>();
        for (Bearing bearing : bearings) {
            if (bearing == last
                    || relate(spec, causality, bearing.event(), last.event()) == Relation.UNDETERMINED) {
                contenders.add(bearing);
            }
        }

        Set<SagaStatus> claimed = new LinkedHashSet<>();
        contenders.forEach(bearing -> claimed.add(bearing.status()));

        SagaStatus status;
        SagaEvent deciding;
        List<SagaEvent> contestedEvents;
        if (claimed.size() == 1) {
            status = last.status();
            deciding = last.event();
            contestedEvents = List.of();
        } else {
            // The evidence does not say which of these came last, so it does not say how
            // the run ended. Fall back to what is settled before the contest and claim no
            // outcome: an observer that picks here reports one of two mutually exclusive
            // endings as fact.
            status = beforeTheContest(bearings, contenders);
            deciding = null;
            contestedEvents = contenders.stream().map(Bearing::event).toList();
        }

        return new EvidenceTimeline(spec, List.copyOf(ordered), List.copyOf(bearings),
                status, deciding, contestedEvents, firstRegression(spec, causality, bearings), causality, superseded);
    }

    /**
     * The status established by the observations the contest does not reach.
     *
     * <p>Never terminal. A terminal reading before a contested position is superseded by
     * whatever the contest resolves to, so it is no more assertable than the contest is.
     */
    private static SagaStatus beforeTheContest(List<Bearing> bearings, List<Bearing> contenders) {
        SagaStatus settled = null;
        for (Bearing bearing : bearings) {
            if (contenders.contains(bearing)) break;
            settled = bearing.status();
        }
        return settled == null || settled.isTerminal() ? SagaStatus.IN_PROGRESS : settled;
    }

    /**
     * The first observation that provably followed an ending, and contradicted it.
     *
     * <h2>Why a drop in rank is not on its own enough</h2>
     * Two things had to be true for this to be a claim about the observed system, and only
     * one of them was checked. The pair has to be provably ordered — it was. But the pair
     * also has to be one the ranking can say something about, and between two
     * <em>non-terminal</em> statuses it cannot.
     *
     * <p>{@link SagaStatus#progressRank} orders the lifecycle, and a run legitimately moves
     * down it while still running. The case that exposed this has a workflow whose stock and
     * payment steps are declared concurrent: payment failed, moving the run to COMPENSATING,
     * and the stock arm — which had been in flight the whole time and owed nothing to the
     * payment arm — then reported success, which reads as IN_PROGRESS. Nothing went
     * backwards. Two independent pieces of work were compared on a scale that does not
     * order them.
     *
     * <p>Worse, whether that comparison happened at all depended on what had arrived. The
     * finding was raised on a three-event prefix and, being an
     * {@link com.example.correlation.domain.AnomalyType.Claim#OCCURRENCE} claim, was never
     * revisited when the fourth event turned up and separated the pair. The same run read
     * live reported {@code TERMINAL_STATE_OVERWRITE}; read as a backlog it reported the
     * compensation failure that was actually there.
     *
     * <h2>What is established, and why it survives more evidence</h2>
     * A terminal status is absolute: the specification says the run is over. An observation
     * provably after one, reporting something lower, is the run contradicting its own
     * ending — which is precisely what {@code TERMINAL_STATE_OVERWRITE} (R04), the rule this
     * finding feeds, is named for. A drop between two non-terminal statuses was never that.
     *
     * <p>Requiring the earlier status to be terminal is also what makes the claim safe to
     * make on partial evidence, which is the only kind this platform ever has. The
     * predicate is monotone: no later observation can make a terminal status non-terminal,
     * nor un-order a pair {@link #relate} has ordered. Inserting a further observation
     * between the two leaves it standing either way — every status ranked at or above a
     * terminal one is itself terminal, so the inserted bearing either drops from the
     * ending itself or becomes a new ending that the later observation still contradicts.
     * So the answer is the same whether the events arrive together or one at a time, which
     * is the invariant this whole class exists to hold.
     *
     * <p>What is given up is a non-terminal backwards move along a dependency edge. Nothing
     * observed on any of the systems validated so far produced one, and establishing it
     * would need evidence that the steps in between were not merely unobserved — evidence
     * an observer of a partial stream does not have.
     */
    private static Regression firstRegression(WorkflowSpec spec, Causality causality, List<Bearing> bearings) {
        for (int i = 1; i < bearings.size(); i++) {
            Bearing earlier = bearings.get(i - 1);
            Bearing later = bearings.get(i);
            if (!earlier.status().isTerminal()) continue;
            if (relate(spec, causality, earlier.event(), later.event()) != Relation.BEFORE) continue;
            if (later.status().progressRank() < earlier.status().progressRank()) {
                return new Regression(earlier.event(), earlier.status(),
                        later.event(), later.status());
            }
        }
        return null;
    }

    /**
     * A total order to walk the run in.
     *
     * <p>Every key is a scalar, deliberately. The dependency relation itself is not
     * transitive — two steps on unrelated branches are mutually un-upstream — and a
     * comparator built on an intransitive relation is undefined behaviour in a sort, not
     * merely imprecise. {@link WorkflowSpec#upstreamStepCount} is its transitive rendering
     * and agrees with it wherever it has an opinion.
     *
     * <p>The event id comes last as a tie-break for determinism only. Nothing is concluded
     * from it: {@link #relate} reports such a pair as undetermined, and every claim this
     * class makes is checked against that rather than against the sort.
     */
    private static Comparator<SagaEvent> businessOrder(WorkflowSpec spec, Causality causality) {
        return Comparator
                .comparing(causality::at, Comparator.nullsLast(Comparator.naturalOrder()))
                // A cause placed at the same instant as its effect still comes first.
                .thenComparingInt(causality::depth)
                .thenComparingInt(event -> spec.classifyEvent(event.getEventType())
                        .map(role -> spec.upstreamStepCount(role.stepId()))
                        .orElse(Integer.MAX_VALUE))
                .thenComparingInt(event -> spec.classifyEvent(event.getEventType())
                        .map(role -> lifecycleRank(role.role()))
                        .orElse(Integer.MAX_VALUE))
                .thenComparing(SagaEvent::getEventId,
                        Comparator.nullsLast(Comparator.naturalOrder()));
    }

    /**
     * What can be proved about the order of two observations, as opposed to what the sort
     * had to decide in order to produce a sequence at all.
     */
    private static Relation relate(WorkflowSpec spec, Causality causality, SagaEvent left, SagaEvent right) {
        if (left == right) return Relation.UNDETERMINED;

        // A declared cause precedes its effects; no clock overrules that (F28).
        if (causality.isAncestor(left, right)) return Relation.BEFORE;
        if (causality.isAncestor(right, left)) return Relation.AFTER;

        Instant leftAt = causality.at(left);
        Instant rightAt = causality.at(right);
        if (leftAt != null && rightAt != null && !leftAt.equals(rightAt)) {
            return leftAt.isBefore(rightAt) ? Relation.BEFORE : Relation.AFTER;
        }

        // Same instant, or an observation with no clock at all. The clock has nothing left
        // to say about this pair; the workflow might.
        Optional<EventRole> leftRole = spec.classifyEvent(left.getEventType());
        Optional<EventRole> rightRole = spec.classifyEvent(right.getEventType());
        if (leftRole.isEmpty() || rightRole.isEmpty()) return Relation.UNDETERMINED;

        String leftStep = leftRole.get().stepId();
        String rightStep = rightRole.get().stepId();
        if (!Objects.equals(leftStep, rightStep)) {
            if (spec.isUpstreamOf(leftStep, rightStep)) return Relation.BEFORE;
            if (spec.isUpstreamOf(rightStep, leftStep)) return Relation.AFTER;
            return Relation.UNDETERMINED;
        }

        int leftRank = lifecycleRank(leftRole.get().role());
        int rightRank = lifecycleRank(rightRole.get().role());
        if (leftRank == rightRank) return Relation.UNDETERMINED;
        return leftRank < rightRank ? Relation.BEFORE : Relation.AFTER;
    }

    /** Where in one step's life an observation falls. */
    private static int lifecycleRank(EventRole.Role role) {
        return switch (role) {
            case STARTED -> 0;
            case RETRY -> 1;
            case COMPLETED, FAILED -> 2;
        };
    }

    /**
     * What each observation says the run's status is, read against every branch of the run
     * rather than on its own.
     *
     * <h2>The assumption this replaces</h2>
     * {@link WorkflowSpec#statusAfter} answers for one observation in isolation, and the run's
     * outcome was whatever the last such answer said. That holds while a run has one thread
     * of work. ShopSaga (2026-10-22) cancelled an order mid-payment and undid two branches at
     * once: the first release to finish read as COMPENSATED although the refund was still
     * open, and the payment reply that arrived after the cancel read as COMPLETED. Both are
     * one event deciding for the whole run. The shape matrix
     * ({@code SagaShapeMatrixTest}) found the same in every shape with two compensation
     * branches or a reply after compensation began, on systems never observed.
     *
     * <h2>Read against all branches</h2>
     * <ul>
     *   <li>Finishing a compensation ends the run only when no compensation is open: none
     *       started and unfinished, and none owed by a compensable step observed to complete.
     *       Until then the run is COMPENSATING.</li>
     *   <li>While compensation is open, a forward step's outcome is not the run's outcome.
     *       It still says the step was done, so its compensation is owed. Once the run has
     *       ended it is read as before: work after the ending.</li>
     * </ul>
     */
    private static List<Bearing> bearingsOf(WorkflowSpec spec, List<SagaEvent> ordered, Set<SagaEvent> superseded) {
        Set<String> compensationsStarted = new LinkedHashSet<>();
        Set<String> compensationsDone = new LinkedHashSet<>();
        Set<String> owed = new LinkedHashSet<>();
        boolean compensating = false;
        List<Bearing> bearings = new ArrayList<>();
        for (SagaEvent event : ordered) {
            Optional<EventRole> role = spec.classifyEvent(event.getEventType());
            Optional<SagaStatus> alone = role.flatMap(spec::statusAfter);
            if (role.isEmpty()) continue;
            String step = role.get().stepId();
            if (spec.isCompensationStep(step)) {
                if (role.get().role() == EventRole.Role.STARTED) compensationsStarted.add(step);
                if (role.get().role() == EventRole.Role.COMPLETED) compensationsDone.add(step);
                compensating = true;
                if (alone.orElse(null) == SagaStatus.COMPENSATED) {
                    Set<String> open = new LinkedHashSet<>(compensationsStarted);
                    open.addAll(owed);
                    open.removeAll(compensationsDone);
                    alone = Optional.of(open.isEmpty() ? SagaStatus.COMPENSATED : SagaStatus.COMPENSATING);
                }
            } else {
                if (role.get().role() == EventRole.Role.COMPLETED) {
                    spec.compensationOf(step).ifPresent(owed::add);
                }
                // Only while compensation is open. After the run has ended, a forward outcome
                // is work done after the ending, and stays the contradiction it is (C4).
                boolean ended = !bearings.isEmpty() && bearings.get(bearings.size() - 1).status().isTerminal();
                if (compensating && !ended) { superseded.add(event); continue; }
            }
            alone.ifPresent(status -> bearings.add(new Bearing(event, status)));
        }
        return bearings;
    }

    /**
     * The observations that put the run into {@code terminal}, read against every branch
     * ({@link #bearingsOf}): a forward reply after compensation began, or one branch's
     * compensation finishing while another is open, is not one.
     */
    public boolean isSupersededByCompensation(SagaEvent event) {
        return superseded.contains(event);
    }

    public List<SagaEvent> endingsAs(SagaStatus terminal) {
        return bearings.stream().filter(b -> b.status() == terminal).map(Bearing::event).toList();
    }

    // ── what the caller asks ──────────────────────────────────────────────────

    /** The status the whole body of evidence implies, independent of read order. */
    public SagaStatus status() {
        return status;
    }

    /** The run's observations in business order. */
    public List<SagaEvent> inBusinessOrder() {
        return ordered;
    }

    /** The earliest instant any observation of this run carries. */
    public Optional<Instant> startedAt() {
        return ordered.stream()
                .map(causality::at)
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder());
    }

    /** The latest instant any observation of this run carries. */
    public Optional<Instant> lastEventAt() {
        return ordered.stream()
                .map(causality::at)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder());
    }

    /**
     * When the run ended, or empty while it has not.
     *
     * <p>The instant of the observation the terminal status rests on — not of whichever
     * event was being processed when the platform worked it out.
     */
    public Optional<Instant> endedAt() {
        return status.isTerminal() && decidingEvent != null
                ? Optional.ofNullable(causality.at(decidingEvent))
                : Optional.empty();
    }

    /**
     * The observations whose relative order the evidence does not settle, when that
     * indecision is what stops the run's outcome from being stated. Empty otherwise.
     */
    public List<SagaEvent> contestedOutcome() {
        return contested;
    }

    public Optional<Regression> regression() {
        return Optional.ofNullable(regression);
    }

    /**
     * The ending, and the observation the evidence puts after it, if there provably is
     * such a pair.
     *
     * <p>The question {@code FORBIDDEN_POST_TERMINAL} means to ask. Asking it of the
     * stored status instead answered "had the platform already concluded the run was over
     * by the time it got round to this event", which is true of every event read after a
     * completion — including the ones that happened before it.
     *
     * <p>Asked of the whole reconstruction, and not of whichever observation is in hand.
     * The arriving event is half of the pair only when it happens to be the <em>later</em>
     * half, so scoping the question to it left every run whose terminal event arrived last
     * unexamined: the same three observations raised the finding on one delivery order and
     * stayed silent on another. That is the shape of the FP-2 defect — a claim decided on a
     * prefix and never revisited — reached through a different predicate, and it breaks I9
     * for a finding I9 does not exempt.
     *
     * <p>The representative pair is the first qualifying terminal, then the first
     * observation provably after it, in {@link #businessOrder}. That order's id tie-break
     * only makes the citation deterministic; {@link #relate} still has to prove the pair.
     * Callers must refresh an existing citation when the fuller reconstruction selects a
     * different pair (F9).
     */
    /**
     * Whether {@code event} is provably after an observation that ended the run — by the same
     * proof {@link #postTerminal()} uses. Such an observation is not a handoff: the graph has
     * no step after an ending, and the observation is already that finding's evidence.
     */
    public boolean isAfterEnding(SagaEvent event) {
        return bearings.stream()
                .filter(bearing -> bearing.status().isTerminal())
                .anyMatch(ended -> relate(spec, causality, ended.event(), event) == Relation.BEFORE);
    }

    /**
     * The same ending reported again by a different channel, which confirms it.
     *
     * <p>ewallet publishes a release or unholding on its business topic and again on its
     * result topic; both map to the event that ended the run, and the second was reported
     * as activity after the ending. A source that processes the step twice repeats it on
     * the same channel -- SPM after resume, which charged the customer twice -- and that is
     * still a finding. Channels are told apart by the topic in the transport position
     * (topic-partition-offset, SagaEventConsumer); a source name cannot, being the system's
     * namespace shared by all its mappings. Without a transport position on either side
     * nothing can be told, and the observation is judged as before.
     */
    private static boolean corroborates(SagaEvent ending, SagaEvent late) {
        if (!Objects.equals(ending.getEventType(), late.getEventType())) return false;
        String endingChannel = channelOf(ending.getTransportRef());
        String lateChannel = channelOf(late.getTransportRef());
        return endingChannel != null && lateChannel != null && !endingChannel.equals(lateChannel);
    }

    /** The topic of a {@code topic-partition-offset} position; topic names may contain '-'. */
    static String channelOf(String transportRef) {
        if (transportRef == null) return null;
        int offsetDash = transportRef.lastIndexOf('-');
        if (offsetDash <= 0) return null;
        int partitionDash = transportRef.lastIndexOf('-', offsetDash - 1);
        return partitionDash <= 0 ? null : transportRef.substring(0, partitionDash);
    }

    /**
     * A compensation's completion repeated in answer to a request the source sent again.
     *
     * <p>ShopSaga (payment-stop, 2026-10-22) re-sent the refund while payment-service was down
     * and payment answered both: two PAYMENT_REFUNDED, one refund row. A repeat of the ending's
     * own compensation step is an acknowledgement while that step has no more completions than
     * starts; one beyond that is work no request asked for, and stays a finding.
     */
    private boolean answersRetriedRequest(SagaEvent ending, SagaEvent late) {
        Optional<EventRole> endRole = spec.classifyEvent(ending.getEventType());
        Optional<EventRole> lateRole = spec.classifyEvent(late.getEventType());
        if (endRole.isEmpty() || lateRole.isEmpty()) return false;
        String step = endRole.get().stepId();
        if (!spec.isCompensationStep(step) || !step.equals(lateRole.get().stepId())
                || lateRole.get().role() != EventRole.Role.COMPLETED) return false;
        long starts = 0, completions = 0;
        for (SagaEvent e : ordered) {
            Optional<EventRole> r = spec.classifyEvent(e.getEventType());
            if (r.isEmpty() || !step.equals(r.get().stepId())) continue;
            if (r.get().role() == EventRole.Role.STARTED) starts++;
            if (r.get().role() == EventRole.Role.COMPLETED) completions++;
        }
        return completions <= starts;
    }

    public Optional<PostTerminal> postTerminal() {
        // EXT-UNMAPPED-POST-TERMINAL: only an observation the workflow can place says the run
        // did something after it ended. One it cannot place has no meaning here -- ewallet's
        // result topic republishes W2N_Holding_Success, deliberately unmapped, after the
        // unholding that ended the run -- and reporting it blamed the source for the
        // platform not knowing what the event was.
        return bearings.stream()
                .filter(bearing -> bearing.status().isTerminal())
                .flatMap(ended -> ordered.stream()
                        .filter(late -> spec.classifyEvent(late.getEventType()).isPresent())
                        .filter(late -> !corroborates(ended.event(), late))
                        .filter(late -> !answersRetriedRequest(ended.event(), late))
                        .filter(late -> relate(spec, causality, ended.event(), late) == Relation.BEFORE)
                        .findFirst()
                        .map(late -> new PostTerminal(ended.event(), ended.status(), late))
                        .stream())
                .findFirst();
    }
}
