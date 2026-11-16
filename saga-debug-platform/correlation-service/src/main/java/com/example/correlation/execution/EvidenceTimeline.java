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

public final class EvidenceTimeline {
    private final WorkflowSpec spec;
    private final List<SagaEvent> ordered;
    private final List<Bearing> bearings;
    private final SagaStatus status;
    private final SagaEvent decidingEvent;
    private final List<SagaEvent> contested;
    private final Regression regression;
    private final Causality causality;

    private final Set<SagaEvent> superseded;

    public record Regression(SagaEvent earlier, SagaStatus from, SagaEvent later, SagaStatus to) {
    }

    public record PostTerminal(SagaEvent terminal, SagaStatus endedAs, SagaEvent late) {
    }

    private record Bearing(SagaEvent event, SagaStatus status) {
    }

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

        Instant at(SagaEvent e) {
            return at.containsKey(e) ? at.get(e) : e.getTimestamp();
        }

        int depth(SagaEvent e) {
            return depth.getOrDefault(e, 0);
        }

        boolean isAncestor(SagaEvent ancestor, SagaEvent event) {
            Set<SagaEvent> seen = new java.util.HashSet<>();
            for (SagaEvent cur = causeOf(event); cur != null && seen.add(cur); cur = causeOf(cur)) {
                if (cur == ancestor) return true;
            }
            return false;
        }
    }

    public static EvidenceTimeline of(WorkflowSpec spec, List<SagaEvent> events) {
        List<SagaEvent> given = events == null ? List.of() : events;
        Causality causality = new Causality(spec, given);
        if (spec == null) {
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
            status = beforeTheContest(bearings, contenders);
            deciding = null;
            contestedEvents = contenders.stream().map(Bearing::event).toList();
        }

        return new EvidenceTimeline(spec, List.copyOf(ordered), List.copyOf(bearings),
                status, deciding, contestedEvents, firstRegression(spec, causality, bearings), causality, superseded);
    }

    private static SagaStatus beforeTheContest(List<Bearing> bearings, List<Bearing> contenders) {
        SagaStatus settled = null;
        for (Bearing bearing : bearings) {
            if (contenders.contains(bearing)) break;
            settled = bearing.status();
        }
        return settled == null || settled.isTerminal() ? SagaStatus.IN_PROGRESS : settled;
    }

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

    private static Comparator<SagaEvent> businessOrder(WorkflowSpec spec, Causality causality) {
        return Comparator
                .comparing(causality::at, Comparator.nullsLast(Comparator.naturalOrder()))

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

    private static Relation relate(WorkflowSpec spec, Causality causality, SagaEvent left, SagaEvent right) {
        if (left == right) return Relation.UNDETERMINED;

        if (causality.isAncestor(left, right)) return Relation.BEFORE;
        if (causality.isAncestor(right, left)) return Relation.AFTER;

        Instant leftAt = causality.at(left);
        Instant rightAt = causality.at(right);
        if (leftAt != null && rightAt != null && !leftAt.equals(rightAt)) {
            return leftAt.isBefore(rightAt) ? Relation.BEFORE : Relation.AFTER;
        }

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

    private static int lifecycleRank(EventRole.Role role) {
        return switch (role) {
            case STARTED -> 0;
            case RETRY -> 1;
            case COMPLETED, FAILED -> 2;
        };
    }

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

                boolean ended = !bearings.isEmpty() && bearings.get(bearings.size() - 1).status().isTerminal();
                if (compensating && !ended) { superseded.add(event); continue; }
            }
            alone.ifPresent(status -> bearings.add(new Bearing(event, status)));
        }
        return bearings;
    }

    public boolean isSupersededByCompensation(SagaEvent event) {
        return superseded.contains(event);
    }

    public List<SagaEvent> endingsAs(SagaStatus terminal) {
        return bearings.stream().filter(b -> b.status() == terminal).map(Bearing::event).toList();
    }

    public SagaStatus status() {
        return status;
    }

    public List<SagaEvent> inBusinessOrder() {
        return ordered;
    }

    public Optional<Instant> startedAt() {
        return ordered.stream()
                .map(causality::at)
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder());
    }

    public Optional<Instant> lastEventAt() {
        return ordered.stream()
                .map(causality::at)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder());
    }

    public Optional<Instant> endedAt() {
        return status.isTerminal() && decidingEvent != null
                ? Optional.ofNullable(causality.at(decidingEvent))
                : Optional.empty();
    }

    public List<SagaEvent> contestedOutcome() {
        return contested;
    }

    public Optional<Regression> regression() {
        return Optional.ofNullable(regression);
    }

    public boolean isAfterEnding(SagaEvent event) {
        return bearings.stream()
                .filter(bearing -> bearing.status().isTerminal())
                .anyMatch(ended -> relate(spec, causality, ended.event(), event) == Relation.BEFORE);
    }

    private static boolean corroborates(SagaEvent ending, SagaEvent late) {
        if (!Objects.equals(ending.getEventType(), late.getEventType())) return false;
        String endingChannel = channelOf(ending.getTransportRef());
        String lateChannel = channelOf(late.getTransportRef());
        return endingChannel != null && lateChannel != null && !endingChannel.equals(lateChannel);
    }

    static String channelOf(String transportRef) {
        if (transportRef == null) return null;
        int offsetDash = transportRef.lastIndexOf('-');
        if (offsetDash <= 0) return null;
        int partitionDash = transportRef.lastIndexOf('-', offsetDash - 1);
        return partitionDash <= 0 ? null : transportRef.substring(0, partitionDash);
    }

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
