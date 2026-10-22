package com.example.correlation.workflow;

import com.example.correlation.domain.SagaStatus;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowSpec {

    private String name;
    private String version;

    /**
     * Which observed systems this specification describes.
     *
     * <p>Empty, and normally left so: a deployment watching one system needs no such
     * statement and nothing about it changes. It exists because a deployment watching two
     * does need one. Event names are not globally unique — {@code ORDER_CREATED} is what
     * an order system's first event is called, in every company that has one — so a second
     * system's specification arriving on the classpath used to make the first system's runs
     * unrecognisable: two workflows claimed the same start event, the registry could not
     * pick one, and it correctly refused to guess.
     *
     * <p>Naming the sources turns that from a collision into a filter. Resolution narrows
     * to the specs that describe the system the observation actually came from, and the
     * ambiguity disappears without anything being inferred.
     *
     * <p>Names observed systems — a mapping's {@code system:} when it declares one, its
     * {@code name} otherwise — not the individual mapping files. A system read by four
     * mappings is one entry here, because it is one id space and one specification
     * describes it.
     *
     * <p>A spec that names no source still applies to any of them, so adding this field
     * changed no existing file.
     */
    private List<String> sources = new ArrayList<>();

    /**
     * The deployment this workflow's services run in, as the runtime names it (Compose
     * project). Optional; when set, only lifecycle facts from that scope are read (V21).
     */
    private String runtimeScope;
    private List<StepSpec> steps;
    private List<StepSpec> compensationSteps;
    private List<TransitionSpec> transitions;
    private Map<String, String> validEventTypes;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<String> getSources() { return sources; }

    public String getRuntimeScope() { return runtimeScope; }
    public void setRuntimeScope(String runtimeScope) { this.runtimeScope = runtimeScope; }
    public void setSources(List<String> sources) {
        this.sources = sources != null ? sources : new ArrayList<>();
    }

    /** Whether this specification describes the system an observation came from. */
    public boolean describesSource(String sourceName) {
        return sources.isEmpty() || (sourceName != null && sources.contains(sourceName));
    }

    /** Whether this specification was written about particular systems rather than any. */
    public boolean isSourceScoped() {
        return !sources.isEmpty();
    }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public List<StepSpec> getSteps() { return steps; }
    public void setSteps(List<StepSpec> steps) { this.steps = steps; }

    public List<StepSpec> getCompensationSteps() { return compensationSteps; }
    public void setCompensationSteps(List<StepSpec> compensationSteps) { this.compensationSteps = compensationSteps; }

    public List<TransitionSpec> getTransitions() { return transitions; }
    public void setTransitions(List<TransitionSpec> transitions) { this.transitions = transitions; }

    public Map<String, String> getValidEventTypes() { return validEventTypes; }
    public void setValidEventTypes(Map<String, String> validEventTypes) { this.validEventTypes = validEventTypes; }


    /**
     * The step an event belongs to: from {@code validEventTypes} where that map names the
     * event, and otherwise from the steps' own {@code events} and {@code outcomes}.
     *
     * <p>The map repeats what the steps already say, and only in part. Consulted alone, it
     * left every event it did not list in no step at all: a workflow written without it —
     * ROOM_RESERVATION, PARTNER_ORDER_SAGA — got null for every event (L16), and ORDER_SAGA,
     * whose map omits every started and retry event, got null for those (L21). SAGA_STUCK then
     * fell back to the default timeout, the adjacency STEP_TIMEOUT skipped the event, and no
     * transition into or out of it was judged. Where the map does name an event it still
     * decides; no shipped specification has it disagree with the steps.
     */
    public String getExpectedStepForEvent(String eventType) {
        String mapped = validEventTypes != null ? validEventTypes.get(eventType) : null;
        if (mapped != null) return mapped;
        return classifyEvent(eventType).map(EventRole::stepId).orElse(null);
    }


    /**
     * Which step an event belongs to, what it says about that step, and under which
     * declared outcome.
     *
     * <p>{@code role} answers "did the step finish, and did it do its job" — the only
     * two things the detectors need. {@code outcome} carries the workflow's own name for
     * what happened, which is what transitions are matched on. For the conventional
     * lifecycle keys these coincide ({@code completed} is outcome SUCCESS,
     * {@code failed} is outcome FAILED), but a workflow may declare any number of named
     * outcomes and route each one differently.
     */
    public record EventRole(String stepId, Role role, String outcome) {
        public enum Role { STARTED, COMPLETED, FAILED, RETRY }

        public EventRole(String stepId, Role role) {
            this(stepId, role, defaultOutcome(role));
        }

        private static String defaultOutcome(Role role) {
            return switch (role) {
                case COMPLETED -> SUCCESS_OUTCOME;
                case FAILED -> FAILED_OUTCOME;
                case STARTED, RETRY -> null;
            };
        }
    }

    /** Conventional outcome names. A workflow may declare others alongside them. */
    public static final String SUCCESS_OUTCOME = "SUCCESS";
    public static final String FAILED_OUTCOME = "FAILED";


    /**
     * Classifies an event against the declared step lifecycles.
     *
     * <p>Resolved from the {@code events} map of each forward and compensation step,
     * which carries the lifecycle role, rather than from {@code validEventTypes},
     * which only carries the step. A step may declare the same event type for more
     * than one role (CREATE_ORDER uses ORDER_CREATED as both {@code started} and
     * {@code completed}); the most conclusive role wins, so such an event is read as
     * a completion and can serve as a predecessor reference.
     *
     * @return the role, or empty when no step declares this event type
     */
    public Optional<EventRole> classifyEvent(String eventType) {
        if (eventType == null) return Optional.empty();

        EventRole best = null;
        for (StepSpec step : allSteps()) {
            if (step.getId() == null) continue;

            if (step.getEvents() != null) {
                for (Map.Entry<String, String> entry : step.getEvents().entrySet()) {
                    if (!eventType.equals(entry.getValue())) continue;
                    EventRole.Role role = toRole(entry.getKey());
                    if (role == null) continue;
                    best = better(best, new EventRole(step.getId(), role));
                }
            }

            if (step.getOutcomes() != null) {
                for (Map.Entry<String, String> entry : step.getOutcomes().entrySet()) {
                    if (!eventType.equals(entry.getValue())) continue;
                    String outcome = entry.getKey();
                    best = better(best, new EventRole(
                            step.getId(), roleOfOutcome(step.getId(), outcome), outcome));
                }
            }
        }
        return Optional.ofNullable(best);
    }


    /**
     * Whether a named outcome means the step did its job.
     *
     * <p>Read from where the outcome routes: an outcome whose transition lands on the
     * FAILED marker is a failure, anything else is the step finishing — including an
     * outcome that routes to a different branch. A loan assessment returning
     * MANUAL_REVIEW completed successfully; it simply routed elsewhere.
     */
    private EventRole.Role roleOfOutcome(String stepId, String outcome) {
        if (FAILED_OUTCOME.equals(outcome)) return EventRole.Role.FAILED;
        boolean routesToFailure = outgoingTargets(stepId, outcome).stream()
                .anyMatch(target -> TERMINAL_TARGETS.get(target) == SagaStatus.FAILED);
        return routesToFailure ? EventRole.Role.FAILED : EventRole.Role.COMPLETED;
    }

    private static EventRole better(EventRole current, EventRole candidate) {
        if (current == null) return candidate;
        return rolePrecedence(candidate.role()) < rolePrecedence(current.role())
                ? candidate : current;
    }


    /**
     * The step that, on success, hands off to {@code stepId}.
     *
     * <p>Answers only for the unambiguous case. Steps reached through a join or through
     * a branch merge have several, and {@link #predecessorSteps} together with
     * {@link #isJoin} describe those.
     *
     * @return the predecessor step id, or empty when none is declared or when the
     *         declaration is ambiguous (several predecessors, i.e. a join)
     */
    public Optional<String> predecessorStep(String stepId) {
        if (transitions == null || stepId == null) return Optional.empty();

        List<String> candidates = predecessorSteps(stepId);
        return candidates.size() == 1 ? Optional.of(candidates.get(0)) : Optional.empty();
    }


    /**
     * Every step that hands off to {@code stepId} on success.
     *
     * <p>{@link #predecessorStep} answers only when there is exactly one, because a
     * single predecessor is the only case where "the previous step" is meaningful. A
     * join has several, and its start is when the last of them finished — which needs
     * the whole list, not a pick.
     */
    public List<String> predecessorSteps(String stepId) {
        if (transitions == null || stepId == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId.equals(t.getTo()))
                .map(TransitionSpec::getFrom)
                .filter(from -> from != null && hasStep(from))
                .distinct()
                .toList();
    }


    /**
     * Whether a step waits for all its predecessors or for whichever one ran.
     *
     * <p>A step with several predecessors is either a join, where independent arms must
     * all finish, or the point where alternative branches come back together, where only
     * one arm ever runs. Treating the second as the first would leave every execution
     * looking like it was missing an arm.
     *
     * <p>The two are told apart by whether the predecessors can reach each other: arms of
     * a fan-out are independent, so neither is upstream of the other and both must
     * complete. Branches of a decision lie on the same chain — the decision step routes
     * one way directly and the other way through an intermediate step — so one is
     * upstream of the other and exactly one of them runs.
     */
    public boolean isJoin(String stepId) {
        List<String> predecessors = predecessorSteps(stepId);
        if (predecessors.size() < 2) return false;

        for (String first : predecessors) {
            for (String second : predecessors) {
                if (first.equals(second)) continue;
                if (isUpstreamOf(first, second)) return false;
            }
        }
        return true;
    }


    /**
     * Whether the specification declares these two steps concurrent.
     *
     * <p>Concurrent means the graph fans out to both on one outcome and never brings
     * either onto the other's path: they are meant to run at the same time, in no
     * particular order. The declaration is the workflow author's, not an inference — a
     * step listing two targets under the <em>same</em> condition says both happen, which
     * is what makes them arms rather than alternatives. Two targets under
     * <em>different</em> conditions are a decision, exactly one arm runs, and the order
     * between them remains meaningful.
     *
     * <h2>What this is for</h2>
     * A run of {@code sylleryum/kafka-microservices-with-saga} has stock and payment both
     * consuming the order event, neither waiting for the other. Whichever answered first
     * was read as a transition into the other:
     *
     * <pre>
     *   declared:  RECEIVE_ORDER -> RESERVE_STOCK  (SUCCESS)
     *              RECEIVE_ORDER -> TAKE_PAYMENT   (SUCCESS)
     *   observed:  TAKE_PAYMENT, then RESERVE_STOCK
     *   answered:  INVALID  ("TAKE_PAYMENT -> RESERVE_STOCK is not declared")
     * </pre>
     *
     * <p>Nothing was wrong. The specification declares no edge between those two steps
     * because there is no handoff between them to declare, and the finding said only which
     * of two independent services happened to answer first — it flipped direction with
     * delivery order, and stood against healthy completed runs.
     *
     * <p>This does not weaken the verdict elsewhere. A decision's branches stay orderable,
     * a join stays downstream of both its arms — {@link #isUpstreamOf} settles that before
     * the fan-out is even looked for — and a step that really did go somewhere the graph
     * forbids is untouched, because no fan-out puts it and its target on separate arms.
     */
    public boolean areConcurrent(String first, String second) {
        if (first == null || second == null || first.equals(second)) return false;
        if (!hasStep(first) || !hasStep(second)) return false;

        // Ordered by the graph, one way or the other, so there is an order to contradict.
        // This is also what keeps a join off this path: it lies below both its arms.
        if (isUpstreamOf(first, second) || isUpstreamOf(second, first)) return false;

        return onSeparateArmsOfOneFanOut(first, second);
    }


    /**
     * Whether one step's single outcome hands off to both of these, down separate arms.
     *
     * <p>Grouped by source <em>and</em> condition: that pairing is what the specification
     * uses to say "both of these happen". Targets that are markers rather than steps are
     * ignored, so this only ever recognises parallelism someone declared between two named
     * steps.
     */
    private boolean onSeparateArmsOfOneFanOut(String first, String second) {
        if (transitions == null) return false;

        // Keyed by the pair rather than by a joined string, so no separator has to be
        // chosen that a step id or a condition could not contain.
        Map<List<String>, Set<String>> armsByFanOut = new LinkedHashMap<>();
        for (TransitionSpec transition : transitions) {
            if (transition.getFrom() == null || transition.getTo() == null) continue;
            if (!hasStep(transition.getTo())) continue;
            armsByFanOut
                    .computeIfAbsent(
                            List.of(transition.getFrom(), String.valueOf(transition.getCondition())),
                            key -> new LinkedHashSet<>())
                    .add(transition.getTo());
        }

        for (Set<String> arms : armsByFanOut.values()) {
            if (arms.size() < 2) continue;
            for (String armOfFirst : arms) {
                if (!leadsTo(armOfFirst, first)) continue;
                for (String armOfSecond : arms) {
                    if (armOfSecond.equals(armOfFirst)) continue;
                    if (leadsTo(armOfSecond, second)) return true;
                }
            }
        }
        return false;
    }


    /** Whether {@code step} is this arm itself or lies somewhere along it. */
    private boolean leadsTo(String arm, String step) {
        return arm.equals(step) || isUpstreamOf(arm, step);
    }


    /**
     * How many steps the declared graph puts before {@code stepId}.
     *
     * <p>A scalar rendering of {@link #isUpstreamOf}: whenever A is upstream of B, every
     * step upstream of A is also upstream of B and A itself is not, so A's count is
     * strictly smaller. That makes the count a transitive stand-in for the dependency
     * relation, which the relation itself is not — two steps on unrelated branches are
     * mutually un-upstream, and sorting a run's events with a comparator built directly on
     * an intransitive relation is undefined behaviour, not merely imprecise.
     *
     * <p>Used to break ties between observations that carry the same instant. It orders
     * them by what the workflow can prove about the work, never by what the observer
     * happened to read first. Steps the graph does not connect share a count, and those
     * are exactly the pairs this platform must not claim an order for.
     *
     * @return 0 for a step with nothing declared before it, including one the graph does
     *         not mention at all
     */
    public int upstreamStepCount(String stepId) {
        if (stepId == null) return 0;
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>(predecessorSteps(stepId));
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            if (current == null || current.equals(stepId) || !visited.add(current)) continue;
            frontier.addAll(predecessorSteps(current));
        }
        return visited.size();
    }


    public boolean hasStep(String stepId) {
        return allSteps().stream().anyMatch(s -> s.getId() != null && s.getId().equals(stepId));
    }

    /** The service the workflow declares for a step, when it declares one. */
    public Optional<String> serviceOf(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .map(StepSpec::getService)
                .filter(service -> service != null && !service.isBlank())
                .findFirst();
    }

    /** The steps that declare {@code stepId} as their predecessor: what runs once it is done. */
    public List<String> stepsAfter(String stepId) {
        if (stepId == null) return List.of();
        return allSteps().stream()
                .map(StepSpec::getId)
                .filter(Objects::nonNull)
                .filter(candidate -> predecessorSteps(candidate).contains(stepId))
                .distinct()
                .toList();
    }


    private List<StepSpec> allSteps() {
        List<StepSpec> all = new ArrayList<>();
        if (steps != null) all.addAll(steps);
        if (compensationSteps != null) all.addAll(compensationSteps);
        return all;
    }

    private static EventRole.Role toRole(String eventsKey) {
        return switch (eventsKey) {
            case "started" -> EventRole.Role.STARTED;
            case "completed" -> EventRole.Role.COMPLETED;
            case "failed" -> EventRole.Role.FAILED;
            case "retry" -> EventRole.Role.RETRY;
            default -> null;
        };
    }

    private static int rolePrecedence(EventRole.Role role) {
        return switch (role) {
            case COMPLETED -> 0;
            case FAILED -> 1;
            case RETRY -> 2;
            case STARTED -> 3;
        };
    }


    /** Whether a transition is declared, not declared, or cannot be judged. */
    public enum TransitionVerdict { VALID, INVALID, UNKNOWN }


    /**
     * A verdict together with what the graph says would have to lie between the two steps.
     *
     * @param stepsInBetween the steps on declared paths from the source to the target,
     *                       endpoints excluded. Non-empty only for an UNKNOWN reached
     *                       because the target is downstream rather than adjacent: it names
     *                       the work whose observations are missing, which is what an
     *                       operator needs in order to see why nothing was concluded
     */
    public record TransitionJudgment(TransitionVerdict verdict, List<String> stepsInBetween) {

        static TransitionJudgment of(TransitionVerdict verdict) {
            return new TransitionJudgment(verdict, List.of());
        }
    }


    /**
     * Judges an observed step-to-step transition against the declared graph.
     *
     * @see #explainTransition for what each verdict rests on
     */
    public TransitionVerdict judgeTransition(String fromStep, String toStep, String condition) {
        return explainTransition(fromStep, toStep, condition).verdict();
    }


    /**
     * Judges an observed step-to-step transition, and says what the judgment rests on.
     *
     * <h2>Three verdicts, and why the third is not a weaker second</h2>
     * VALID means the graph declares this handoff. INVALID means the graph declares where
     * this step's outcome goes, and it is not here. UNKNOWN means the observations in hand
     * do not settle the question — which is a statement about the evidence, never about
     * the observed system, and so raises nothing.
     *
     * <h2>The distinction this was missing</h2>
     * A wrong transition and a missing observation are not the same claim, and this method
     * used to return INVALID for both. Any pair of observed events that was not a declared
     * edge was condemned:
     *
     * <pre>
     *   observed:  ORDER_CREATED, INVENTORY_RESERVED
     *   declared:  CREATE_ORDER -> PROCESS_PAYMENT -> RESERVE_INVENTORY
     *   answered:  INVALID  ("CREATE_ORDER -> RESERVE_INVENTORY is not declared")
     * </pre>
     *
     * <p>But the run had not gone anywhere the graph forbids. It had gone exactly where the
     * graph says, and the platform had not yet read {@code PAYMENT_COMPLETED} — a partition
     * it was behind on, a retry in flight, a topic assigned to a slower thread. The finding
     * described the observer's own incompleteness and filed it against the observed system,
     * and it stood permanently: as an occurrence claim it was never withdrawn when the
     * missing event turned up moments later.
     *
     * <p>So reachability is the question, not adjacency. If the target is downstream of the
     * source along the declared graph, then a path exists that explains what was seen, and
     * the only thing established is that the steps along it have not been observed. That is
     * not evidence of anything having gone wrong, so the answer is UNKNOWN and the steps in
     * between are reported alongside it.
     *
     * <p>INVALID is reserved for what it names: the source step reported an outcome, the
     * graph says where that outcome goes, and the target is not there and cannot be reached
     * from there. A loan assessment that reported APPROVED and then produced a human review
     * really did go somewhere it should not have — APPROVED routes to disbursement, and no
     * path from disbursement reaches review.
     *
     * @param condition the outcome the <em>source</em> step reported, which is what a
     *                  declared edge's condition names. Null is read as "any outcome"
     */
    public TransitionJudgment explainTransition(String fromStep, String toStep, String condition) {
        if (transitions == null || transitions.isEmpty()) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        // A step handing off to itself is not a transition, so there is no transition to
        // judge: two observations of one step's own lifecycle. A step that contradicts
        // itself is a real finding, and MULTI_TERMINAL_CONFLICT is the one that makes it.
        if (fromStep != null && fromStep.equals(toStep)) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        boolean anyFromThisStep = transitions.stream()
                .anyMatch(t -> t.getFrom() != null && t.getFrom().equals(fromStep));
        if (!anyFromThisStep) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        boolean exactMatch = transitions.stream().anyMatch(t ->
                t.getFrom().equals(fromStep) &&
                t.getTo().equals(toStep) &&
                (condition == null || condition.equals(t.getCondition())));
        if (exactMatch) return TransitionJudgment.of(TransitionVerdict.VALID);

        // No special case for FAILED here. One stood here, meant for "a failure may route to
        // a target declared without a condition", and it accepted any edge between the two
        // steps whatever its condition -- so a failed payment followed by inventory work was
        // VALID because PROCESS_PAYMENT -> RESERVE_INVENTORY exists for SUCCESS (ROADMAP L22).
        // The case it was written for is unconditionedTargets below.

        // Two steps the specification declares concurrent are not ordered by it, so seeing
        // one after the other contradicts nothing it says. Everything below asks where the
        // source step's outcome is declared to go and treats a target that is not there as
        // the execution disobeying its specification -- a question that only makes sense
        // where an order was declared in the first place. See #areConcurrent.
        if (areConcurrent(fromStep, toStep)) {
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        // Where this outcome goes, as the graph declares it -- markers included, because
        // "this outcome ends the run" is itself a declaration. An edge declared without a
        // condition applies whatever the step reported, so it belongs here too.
        Set<String> declared = new LinkedHashSet<>(outgoingTargets(fromStep, condition));
        declared.addAll(unconditionedTargets(fromStep));
        if (declared.isEmpty()) {
            // The step is in the graph but nothing is declared for this outcome. A gap in
            // the specification is not a fault in the execution.
            return TransitionJudgment.of(TransitionVerdict.UNKNOWN);
        }

        Set<String> successors = stepSuccessors(declared);
        if (successors.contains(toStep)) {
            // Reached through the COMPENSATING marker rather than by a step-to-step edge:
            // the graph routes the failure into compensation and this is a compensation
            // step, so the handoff is declared.
            return TransitionJudgment.of(TransitionVerdict.VALID);
        }
        if (successors.isEmpty()) {
            // Declared, and declared to end the run. No step can legitimately follow, so a
            // step following is the execution contradicting its own specification.
            return TransitionJudgment.of(TransitionVerdict.INVALID);
        }

        Set<String> downstream = forwardClosure(successors);
        if (!downstream.contains(toStep)) {
            return TransitionJudgment.of(TransitionVerdict.INVALID);
        }

        // Reachable, so the run went where the graph allows and the steps along the way
        // were simply not observed. Those steps are the missing evidence, named.
        List<String> inBetween = downstream.stream()
                .filter(step -> !step.equals(toStep))
                .filter(step -> forwardClosure(Set.of(step)).contains(toStep))
                .toList();
        return new TransitionJudgment(TransitionVerdict.UNKNOWN, inBetween);
    }


    /** Targets of edges out of {@code stepId} that name no condition. */
    private List<String> unconditionedTargets(String stepId) {
        if (transitions == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId != null && stepId.equals(t.getFrom()))
                .filter(t -> t.getCondition() == null)
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }


    /**
     * The steps a set of declared targets actually hands work to.
     *
     * <p>Most targets are steps and stand for themselves. {@code COMPENSATING} is not a
     * step but a phase, and the work done in it is the compensation steps, so a failure
     * routed there hands off to any of them. Reading the marker as "nothing follows" is
     * what would make an ordinary compensation look like a transition the workflow forbids.
     *
     * <p>The other markers are read as nothing following, which is exactly what they mean:
     * COMPLETED, FAILED and COMPENSATED end the run.
     */
    private Set<String> stepSuccessors(Set<String> declaredTargets) {
        Set<String> successors = new LinkedHashSet<>();
        for (String target : declaredTargets) {
            if (hasStep(target)) {
                successors.add(target);
            } else if (COMPENSATING_MARKER.equals(target) && compensationSteps != null) {
                compensationSteps.stream()
                        .map(StepSpec::getId)
                        .filter(Objects::nonNull)
                        .forEach(successors::add);
            }
        }
        return successors;
    }


    /**
     * Every step reachable from {@code seeds} by following declared edges, seeds included.
     *
     * <p>Unconstrained by condition past the first hop, deliberately: the outcomes of the
     * steps in between were not observed either, so any of their declared edges could have
     * been the one taken. Narrowing them would be the platform assuming an outcome it never
     * saw in order to reach a verdict.
     */
    private Set<String> forwardClosure(Set<String> seeds) {
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>(seeds);
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            if (current == null || !visited.add(current)) continue;
            if (transitions == null) continue;
            Set<String> targets = transitions.stream()
                    .filter(t -> current.equals(t.getFrom()))
                    .map(TransitionSpec::getTo)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            frontier.addAll(stepSuccessors(targets));
        }
        return visited;
    }


    /** Pass/fail reading for callers that do not distinguish UNKNOWN; UNKNOWN passes. */
    public boolean isValidTransition(String fromStep, String toStep, String condition) {
        return judgeTransition(fromStep, toStep, condition) != TransitionVerdict.INVALID;
    }


    /**
     * Whether {@code candidate} lies on the declared SUCCESS path leading into
     * {@code step} — whether work at the candidate step had to happen before work here.
     *
     * <p>Used to tell "earlier in this execution" from "earlier and on the same
     * dependency path". Two steps on unrelated branches can be ordered in time without
     * either being upstream of the other.
     */
    public boolean isUpstreamOf(String candidate, String step) {
        if (candidate == null || step == null || candidate.equals(step)) return false;

        // Breadth-first over every predecessor, so a step reachable through one arm of a
        // join still counts as upstream. Walking a single chain would miss it.
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>(predecessorSteps(step));
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            if (current == null || !visited.add(current)) continue;
            if (candidate.equals(current)) return true;
            frontier.addAll(predecessorSteps(current));
        }
        return false;
    }


    public int getStepTimeoutSeconds(String stepId) {
        if (steps != null) {
            for (StepSpec step : steps) {
                if (step.getId().equals(stepId)) {
                    return parseTimeout(step.getTimeout());
                }
            }
        }
        if (compensationSteps != null) {
            for (StepSpec step : compensationSteps) {
                if (step.getId().equals(stepId)) {
                    return parseTimeout(step.getTimeout());
                }
            }
        }
        return 30; 
    }

    private int parseTimeout(String timeout) {
        if (timeout == null) return 30;
        timeout = timeout.trim().toLowerCase();
        if (timeout.endsWith("s")) {
            return Integer.parseInt(timeout.replace("s", ""));
        } else if (timeout.endsWith("m")) {
            return Integer.parseInt(timeout.replace("m", "")) * 60;
        }
        return 30;
    }

    

    /**
     * Every event type any step declares as its retry signal.
     *
     * <p>Replaces a hard-coded {@code PAYMENT_RETRY} constant in the detectors: which
     * events mean "attempted again" is a property of the workflow, not of the engine.
     */
    public Set<String> retryEventTypes() {
        return allSteps().stream()
                .map(StepSpec::getEvents)
                .filter(Objects::nonNull)
                .map(events -> events.get("retry"))
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }


    /**
     * The attempt budget the workflow declares for a step, if any.
     *
     * <p>Read so that retry findings can state how the observed count compares with
     * the declared policy, instead of only against an engine-side threshold.
     */
    public Optional<Integer> declaredMaxAttempts(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .map(StepSpec::getRetryPolicy)
                .filter(Objects::nonNull)
                .map(RetryPolicySpec::getMaxAttempts)
                .filter(Objects::nonNull)
                .findFirst();
    }



    /**
     * Pseudo-steps that {@code transitions} uses to name a saga outcome rather than a
     * unit of work. They are the bridge between the declared graph and SagaStatus.
     */
    private static final Map<String, SagaStatus> TERMINAL_TARGETS = Map.of(
            "COMPLETED", SagaStatus.COMPLETED,
            "FAILED", SagaStatus.FAILED,
            "COMPENSATING", SagaStatus.COMPENSATING,
            "COMPENSATED", SagaStatus.COMPENSATED);

    private static final String START_MARKER = "STARTED";

    /** The one marker that is a phase rather than an ending: work happens in it. */
    private static final String COMPENSATING_MARKER = "COMPENSATING";


    /**
     * The event types that begin an execution of this workflow.
     *
     * <p>Derived from the {@code STARTED -> firstStep} transition rather than declared
     * separately, so it cannot disagree with the graph. Replaces the hard-coded
     * {@code "ORDER_CREATED"} check that decided whether an event was an orphan.
     */
    public Set<String> startEventTypes() {
        if (transitions == null) return Set.of();
        return transitions.stream()
                .filter(t -> START_MARKER.equals(t.getFrom()))
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .flatMap(stepId -> lifecycleEvents(stepId).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }


    /** Every event type any step of this workflow declares, in any role. */
    public Set<String> declaredEventTypes() {
        return allSteps().stream()
                .flatMap(WorkflowSpec::allDeclaredEvents)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Stream<String> allDeclaredEvents(StepSpec step) {
        return Stream.concat(
                        step.getEvents() != null ? step.getEvents().values().stream() : Stream.empty(),
                        step.getOutcomes() != null ? step.getOutcomes().values().stream() : Stream.empty())
                .filter(Objects::nonNull);
    }


    /**
     * Whether the specification says this step ever reports that it finished.
     *
     * <p>Asked before any finding is raised about a completion that has not been seen.
     * "No completion event observed" reads as a fault only against a specification that
     * declares one; against a system whose step publishes a start and nothing else, the
     * same silence is the system working exactly as declared. Without this question the
     * platform was reading its own expectation as the system's obligation — and one real
     * system does publish {@code started} and no {@code completed} for its rollback, so
     * every compensating run there was reported as unacknowledged, permanently, with no
     * evidence for the claim beyond the absence the declaration had already explained.
     *
     * <p>Reads the same declarations {@link #classifyEvent} does, so a step whose
     * completion arrives as a named outcome counts as declaring one: what matters is
     * whether any event the step can publish would be read as it finishing.
     *
     * @return false for an unknown step, which cannot have declared anything
     */
    public boolean declaresCompletionEvent(String stepId) {
        if (stepId == null) return false;
        for (StepSpec step : allSteps()) {
            if (step.getId() == null || !step.getId().equals(stepId)) continue;

            if (step.getEvents() != null && step.getEvents().keySet().stream()
                    .map(WorkflowSpec::toRole)
                    .anyMatch(role -> role == EventRole.Role.COMPLETED)) {
                return true;
            }
            if (step.getOutcomes() != null && step.getOutcomes().keySet().stream()
                    .anyMatch(outcome -> roleOfOutcome(stepId, outcome) == EventRole.Role.COMPLETED)) {
                return true;
            }
        }
        return false;
    }


    /** The compensation step that undoes this forward step, when one is declared. */
    public Optional<String> compensationOf(String stepId) {
        if (steps == null || stepId == null) return Optional.empty();
        return steps.stream().filter(s -> stepId.equals(s.getId()))
                .map(StepSpec::getCompensation).filter(Objects::nonNull).findFirst();
    }

    public boolean isCompensationStep(String stepId) {
        return compensationSteps != null && compensationSteps.stream()
                .anyMatch(s -> s.getId() != null && s.getId().equals(stepId));
    }


    /**
     * The saga status implied by an observation, or empty when the observation says
     * nothing about it.
     *
     * <p>This is where the workflow's shape replaces hard-coded event names. Whether a
     * step's success ends the saga is read from its outgoing SUCCESS transition: if it
     * points at the COMPLETED marker the saga is done, if it points at another step the
     * saga is still running. The previous implementation answered the same question
     * with {@code COMPLETED_EVENTS = Set.of("INVENTORY_RESERVED")} — true only because
     * reserving inventory happened to be the last step of the only workflow that
     * existed, and wrong for any workflow with a step after it.
     *
     * <p>Compensation steps are read structurally: starting one means the saga is
     * compensating, finishing one means it is compensated, and a failed compensation
     * leaves it compensating rather than resolved.
     */
    public Optional<SagaStatus> statusAfter(EventRole role) {
        if (role == null) return Optional.empty();

        if (isCompensationStep(role.stepId())) {
            return switch (role.role()) {
                case STARTED -> Optional.of(SagaStatus.COMPENSATING);
                case COMPLETED -> Optional.of(SagaStatus.COMPENSATED);
                case FAILED -> Optional.of(SagaStatus.COMPENSATING);
                case RETRY -> Optional.empty();
            };
        }

        if (role.role() == EventRole.Role.STARTED) return Optional.of(SagaStatus.IN_PROGRESS);
        if (role.role() == EventRole.Role.RETRY) return Optional.empty();

        // Matched on the workflow's own outcome name, so a step with three declared
        // outcomes routes three different ways. SUCCESS and FAILED are just the two
        // conventional names, not the only ones the engine understands.
        String condition = role.outcome() != null ? role.outcome() : SUCCESS_OUTCOME;

        // Every declared target has to agree on what this outcome means. A step that
        // fans out to two further steps agrees (both mean "still running"); a step whose
        // targets disagree is under-specified, and picking one of them — which is what
        // findFirst() used to do, silently, in YAML order — would invent an answer.
        Set<SagaStatus> implied = outgoingTargets(role.stepId(), condition).stream()
                .map(target -> TERMINAL_TARGETS.getOrDefault(target, SagaStatus.IN_PROGRESS))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return implied.size() == 1 ? Optional.of(implied.iterator().next()) : Optional.empty();
    }


    /**
     * The steps a run is waiting on once this observation has happened.
     *
     * <p>A step that has started or is retrying is still the one in progress. A step that
     * has reported an outcome hands over to the steps its transitions name for that
     * outcome; the run is now waiting on those, not on the one that just finished. Empty
     * when the outcome leads only to a terminal marker, or the event is not this
     * workflow's.
     */
    public List<String> stepsAwaitedAfter(String eventType) {
        Optional<EventRole> role = classifyEvent(eventType);
        if (role.isEmpty()) return List.of();
        EventRole r = role.get();
        if (r.role() == EventRole.Role.STARTED || r.role() == EventRole.Role.RETRY) {
            return List.of(r.stepId());
        }
        String condition = r.outcome() != null ? r.outcome()
                : r.role() == EventRole.Role.FAILED ? "FAILED" : SUCCESS_OUTCOME;
        Set<String> stepIds = allSteps().stream().map(StepSpec::getId).collect(Collectors.toSet());
        return outgoingTargets(r.stepId(), condition).stream().filter(stepIds::contains).toList();
    }

    private List<String> outgoingTargets(String stepId, String condition) {
        if (transitions == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId != null && stepId.equals(t.getFrom()))
                .filter(t -> condition.equals(t.getCondition()))
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }


    private Set<String> lifecycleEvents(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .flatMap(WorkflowSpec::allDeclaredEvents)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RetryPolicySpec {
        private Integer maxAttempts;
        private String backoff;

        public Integer getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }

        public String getBackoff() { return backoff; }
        public void setBackoff(String backoff) { this.backoff = backoff; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StepSpec {
        private String id;
        private String service;
        private String timeout;
        private String compensation;
        private RetryPolicySpec retryPolicy;
        private Map<String, String> events;

        /**
         * Named business outcomes of this step: outcome name to event type.
         *
         * <p>Open-ended, unlike {@code events}, whose keys are fixed lifecycle words.
         * Each name is matched against transition conditions, so a step can route to a
         * different successor per outcome instead of only success-or-failure.
         */
        private Map<String, String> outcomes;

        public Map<String, String> getOutcomes() { return outcomes; }
        public void setOutcomes(Map<String, String> outcomes) { this.outcomes = outcomes; }

        public RetryPolicySpec getRetryPolicy() { return retryPolicy; }
        public void setRetryPolicy(RetryPolicySpec retryPolicy) { this.retryPolicy = retryPolicy; }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public String getService() { return service; }
        public void setService(String service) { this.service = service; }

        public String getTimeout() { return timeout; }
        public void setTimeout(String timeout) { this.timeout = timeout; }

        public String getCompensation() { return compensation; }
        public void setCompensation(String compensation) { this.compensation = compensation; }

        public Map<String, String> getEvents() { return events; }
        public void setEvents(Map<String, String> events) { this.events = events; }
    }

    public static class TransitionSpec {
        private String from;
        private String to;
        private String condition;

        public String getFrom() { return from; }
        public void setFrom(String from) { this.from = from; }

        public String getTo() { return to; }
        public void setTo(String to) { this.to = to; }

        public String getCondition() { return condition; }
        public void setCondition(String condition) { this.condition = condition; }
    }
}
