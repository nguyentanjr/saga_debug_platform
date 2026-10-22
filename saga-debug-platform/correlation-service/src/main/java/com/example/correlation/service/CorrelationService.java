package com.example.correlation.service;

import com.example.correlation.domain.AnomalyType;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.EvidenceTimeline;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Optional;

@Service
public class CorrelationService {

    private static final Logger log = LoggerFactory.getLogger(CorrelationService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyDetectionService anomalyDetectionService;
    private final DeduplicationCache deduplicationCache;
    private final WorkflowRegistry workflowRegistry;
    private final ObjectMapper objectMapper;


    public CorrelationService(SagaInstanceRepository sagaInstanceRepository,
                              SagaEventRepository sagaEventRepository,
                              AnomalyDetectionService anomalyDetectionService,
                              DeduplicationCache deduplicationCache,
                              WorkflowRegistry workflowRegistry,
                              ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyDetectionService = anomalyDetectionService;
        this.deduplicationCache = deduplicationCache;
        this.workflowRegistry = workflowRegistry;
        this.objectMapper = objectMapper;
    }

    /** Kept for callers that have no causal metadata to report. */
    @Transactional
    public void processEvent(String eventId, String sagaId, String eventType,
                             String stepName, String serviceName,
                             Instant eventTimestamp, String payload,
                             Instant receivedAt) {
        processEvent(IngestedEvent.withoutCausality(eventId, sagaId, eventType, stepName,
                serviceName, eventTimestamp, payload, receivedAt));
    }

    @Transactional
    public void processEvent(IngestedEvent observation) {
        String eventId = observation.eventId();
        String sagaId = observation.sagaId();
        String eventType = observation.eventType();
        String stepName = observation.stepName();
        Instant eventTimestamp = observation.eventTime();

        // Already stored. Whether that means the observed system delivered this event twice
        // or this platform read one record twice is decided by the record each observation
        // came in on, not by the fact that a lookup hit.
        Optional<SagaEvent> alreadyStored = sagaEventRepository.findBySourceNameAndEventId(
                observation.sourceName(), eventId);
        if (alreadyStored.isPresent()) {
            String storedOn = alreadyStored.get().getTransportRef();
            String arrivedOn = observation.transportRef();

            if (storedOn != null && storedOn.equals(arrivedOn)) {
                // The same record, read again: an offset reset, a restart, a reprocess.
                // Nothing happened in the observed system, so nothing is reported against
                // it. The event is not stored twice either — that has not changed.
                log.info("Re-read of stored event {} for saga {} on the same record {}; "
                                + "observer-side replay, not a duplicate delivery.",
                        eventId, sagaId, storedOn);
                return;
            }

            log.info("Duplicate event {} for saga {} (DB hit). Skipping.", eventId, sagaId);
            anomalyDetectionService.detectDuplicateEvent(sagaId, eventId, eventType,
                    storedOn, arrivedOn);
            return;
        }

        // Locked, not only versioned: RCA locks this row to read the run's findings, and the
        // withdrawals below are written before this transaction commits. With only the
        // version check an analysis could read a finding this ingest had already withdrawn
        // and stamp its diagnosis after the withdrawal, so the stale-diagnosis sweep never saw
        // it (EXT-OVERLOAD-FALSE-FINDINGS: 10 WRONG_SAGA_CORRELATION_KEY on 2000 ev/s).
        SagaInstance saga = sagaInstanceRepository.findByIdForUpdate(sagaId).orElse(null);

        // Loaded once and reused by every detector below. Each detector used to issue
        // its own findBySagaIdOrderBy... for the same rows, so processing one event
        // read the saga's whole history five to six times over.
        List<SagaEvent> priorEvents = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);

        // Resolved per execution: by the saga's workflow type once it is known, and by
        // the event's own start-event membership when creating the saga.
        // Narrowed by the system the observation came from, so a second observed system's
        // specification cannot make this one's runs unrecognisable by reusing an event name.
        WorkflowSpec workflow = workflowRegistry
                .resolve(saga != null ? saga.getWorkflowType() : null,
                        observation.declaredWorkflow(),
                        observation.sourceName(),
                        eventType)
                .orElse(null);

        if (saga == null) {
            saga = new SagaInstance();
            saga.setSagaId(sagaId);
            // The namespace half of the identity, stored beside the key it is part of so
            // queries can filter by observed system without parsing strings.
            saga.setSourceName(observation.sourceName());
            saga.setStatus(SagaStatus.STARTED);
            saga.setStartTime(eventTimestamp);
            saga.setLastUpdated(eventTimestamp);
            if (workflow != null) {
                saga.setWorkflowType(workflow.getName());
            }

            saga.setOrderId(observation.businessKey());
            log.info("New saga detected: {} (workflow={})", sagaId, saga.getWorkflowType());
        } else {
            adoptWorkflowIfNowKnown(saga, workflow, eventType);
        }

        saga.recordWorkflowConflict(workflowRegistry.resolveFromEvidence(
                observation.declaredWorkflow(), observation.sourceName(), eventType)
                .map(WorkflowSpec::getName).orElse(null));
        saga.recordWorkflowConflict(workflowRegistry.byType(observation.declaredWorkflow())
                .map(WorkflowSpec::getName).orElse(null));
        if (saga.hasWorkflowConflict()) {
            log.error("Execution {} has conflicting workflows {}; withholding workflow conclusions",
                    sagaId, saga.getWorkflowConflicts());
            workflow = null;
        }

        SagaEvent sagaEvent = observedEventOf(observation);

        // Every observation of this run, this one included. Assembled before anything is
        // concluded, because everything below is concluded from the whole of it.
        List<SagaEvent> allEvents = new ArrayList<>(priorEvents);
        allEvents.add(sagaEvent);

        // The run reconstructed from all its evidence, in the order the workflow can prove
        // things happened. Status is no longer advanced one arrival at a time: an event
        // that arrives late is an event that happened early, and folding it in as though it
        // were the newest thing to happen is what made the observer's read order look like
        // the observed system's behaviour.
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, allEvents);
        SagaStatus newStatus = timeline.status();
        saga.setStatus(newStatus);

        // Both bounds move one way only. A late arrival can reveal that the run began
        // earlier than anything seen so far and can never mean it began later; the same in
        // reverse for how far into the run the platform has observed. Assigning the current
        // event's instant to either, as this did, walked them backwards on every reordered
        // delivery -- and lastUpdated walking backwards is a saga the stuck-run pass then
        // reports for standing still.
        Instant earliest = timeline.startedAt().orElse(eventTimestamp);
        if (saga.getStartTime() == null || earliest.isBefore(saga.getStartTime())) {
            saga.setStartTime(earliest);
        }
        Instant latest = timeline.lastEventAt().orElse(eventTimestamp);
        if (saga.getLastUpdated() == null || latest.isAfter(saga.getLastUpdated())) {
            saga.setLastUpdated(latest);
        }

        // The instant of the observation the outcome rests on, and nothing at all while the
        // run has no outcome. It used to be the instant of whichever event was in hand when
        // a terminal status was worked out -- on a reordered stream, some other event
        // entirely -- and it was left standing when a later reading said the run had not
        // ended, so a saga could report IN_PROGRESS and an end time together.
        saga.setEndTime(timeline.endedAt().orElse(null));

        sagaInstanceRepository.save(saga);
        sagaEventRepository.save(sagaEvent);

        log.info("Saga {} | event={} | step={} | status={}", sagaId, eventType, stepName, newStatus);

        // Keep the observations. A configuration disagreement is not a source-system anomaly.
        if (saga.hasWorkflowConflict()) return;

        // Claims about how this run behaved, each read off the reconstruction rather than
        // off whatever difference this one arrival happened to make to what was stored.
        if (timeline.endedAt().isPresent()) {
            anomalyDetectionService.withdrawMissingTerminal(sagaId, newStatus);
        }
        // Asked of the whole evidence every time, and answered both ways: a pair the clocks
        // ordered can be reordered by a causal link that arrives later (F28).
        timeline.postTerminal().ifPresentOrElse(pair ->
                anomalyDetectionService.detectForbiddenPostTerminal(sagaId,
                        pair.endedAs(), pair.terminal(), pair.late()),
                () -> anomalyDetectionService.withdrawUnprovenOrder(sagaId, AnomalyType.FORBIDDEN_POST_TERMINAL));
        timeline.regression().ifPresentOrElse(regression ->
                anomalyDetectionService.detectStateRegression(sagaId, regression),
                () -> anomalyDetectionService.withdrawUnprovenOrder(sagaId, AnomalyType.STATE_REGRESSION));
        if (!timeline.contestedOutcome().isEmpty()) {
            anomalyDetectionService.detectIndeterminateOrder(sagaId, timeline.contestedOutcome());
        }

        List<SagaEvent> sagaEvents = timeline.inBusinessOrder();

        // Given the unordered list deliberately: this is the one detector whose subject is
        // the two orders disagreeing, and it sorts by arrival itself to see that.
        anomalyDetectionService.detectOutOfOrder(sagaId, allEvents);
        anomalyDetectionService.detectRetryBurst(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectMultiTerminalConflict(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectDuplicateCompensation(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectMissingTraceContext(sagaId, sagaEvents);
        // Asked of the run, not of the arrival that created it. Whether a start event has
        // been observed is a claim about the evidence in hand, and the evidence changes
        // with every event -- including by acquiring the start event the finding says is
        // absent, which is what withdraws it.
        anomalyDetectionService.detectOrphanEvent(workflow, sagaId, sagaEvents);
        anomalyDetectionService.withdrawAcknowledgedCompensation(workflow, sagaId, sagaEvents);
        anomalyDetectionService.withdrawStartedCompensation(workflow, sagaId, sagaEvents);
        anomalyDetectionService.withdrawDisprovenSilence(workflow, sagaId, sagaEvents);

        judgeTransitions(workflow, sagaId, timeline);
    }

    /**
     * Judges every handoff the run made, against the graph.
     *
     * <p>Every consecutive pair in business order, not only the pair ending at the event
     * that has just arrived. Judging that one pair made whether a fault was found depend on
     * which event happened to be read last: an assessment that wrongly produced a human
     * review went unreported whenever its own event arrived after the review's, because the
     * two were never adjacent at the moment either of them was processed. A finding has to
     * be a function of the evidence, so the whole reconstructed sequence is walked, and the
     * repository's per-type check keeps one finding from being filed twice.
     *
     * <p>Cheap: the sequence is already sorted for everything else on this path, and an
     * execution holds a handful of events.
     */
    private void judgeTransitions(WorkflowSpec workflow, String sagaId, EvidenceTimeline timeline) {
        if (workflow == null) return;

        List<SagaEvent> inOrder = timeline.inBusinessOrder();
        Set<String> invalid = new HashSet<>();
        for (int i = 1; i < inOrder.size(); i++) {
            SagaEvent previous = inOrder.get(i - 1);
            SagaEvent current = inOrder.get(i);
            if (isCompensationStepEvent(workflow, current.getEventType())) continue;
            // An observation after the run's ending is FORBIDDEN_POST_TERMINAL's evidence, not
            // a handoff. Judging it here filed the same event twice under two claims, and RCA
            // ranked the routing reading first (SC13, SC21).
            if (timeline.isAfterEnding(current)) continue;
            // A forward reply read while compensation was open is not the run moving on, so
            // it neither starts nor ends a handoff (ShopSaga customer-cancel).
            if (timeline.isSupersededByCompensation(current)
                    || timeline.isSupersededByCompensation(previous)) continue;

            String previousStepName = workflow.getExpectedStepForEvent(previous.getEventType());
            String currentStepName = workflow.getExpectedStepForEvent(current.getEventType());
            if (previousStepName == null || currentStepName == null) continue;

            // The condition of a declared edge names the outcome of the step it leaves, so
            // the outcome to read is the earlier observation's, not the later one's. Reading
            // the later one's meant a loan assessment that reported APPROVED was judged
            // against the edge for whatever the *next* step reported -- SUCCESS -- which
            // APPROVED is not, so every healthy run of a workflow with named outcomes was
            // condemned for the transition it correctly made.
            handoffCondition(workflow, previous.getEventType()).ifPresent(condition -> {
                if (anomalyDetectionService.detectInvalidTransition(workflow,
                        sagaId, previousStepName, currentStepName, condition,
                        current.getEventId(), current.getTimestamp())) {
                    invalid.add(AnomalyDetectionService.transitionKey(
                            AnomalyDetectionService.handoff(previousStepName, currentStepName, condition),
                            current.getEventId()));
                }
            });
        }
        anomalyDetectionService.withdrawUnjudgedTransitions(sagaId, invalid);
    }

    /**
     * The outcome the previous step reported, when it reported one.
     *
     * <p>Empty for an observation that only says a step began or was attempted again: the
     * step has not handed off to anything yet, so there is no transition to judge. Assuming
     * SUCCESS for those would be the platform supplying an outcome it never saw.
     */
    private Optional<String> handoffCondition(WorkflowSpec workflow, String eventType) {
        return workflow.classifyEvent(eventType)
                .filter(role -> role.role() == WorkflowSpec.EventRole.Role.COMPLETED
                        || role.role() == WorkflowSpec.EventRole.Role.FAILED)
                // The workflow's own outcome name, so a step declaring APPROVED /
                // MANUAL_REVIEW / REJECTED is judged against the transition declared for
                // each, rather than being flattened to success-or-failure.
                .map(role -> role.outcome() != null
                        ? role.outcome()
                        : WorkflowSpec.SUCCESS_OUTCOME);
    }

    private SagaEvent observedEventOf(IngestedEvent observation) {
        SagaEvent sagaEvent = new SagaEvent();
        sagaEvent.setEventId(observation.eventId());
        sagaEvent.setSagaId(observation.sagaId());
        sagaEvent.setSourceName(observation.sourceName());
        sagaEvent.setCorrelationBasis(observation.correlationBasis());
        sagaEvent.setEventType(observation.eventType());
        sagaEvent.setStepName(observation.stepName());
        sagaEvent.setServiceName(observation.serviceName());
        sagaEvent.setTimestamp(observation.eventTime());
        sagaEvent.setEventTimeSource(observation.eventTimeSource());
        sagaEvent.setPayload(observation.payload());
        sagaEvent.setReceivedAt(observation.receivedAt());
        sagaEvent.setCausationId(observation.causationId());
        sagaEvent.setSpanId(observation.spanId());
        sagaEvent.setParentSpanId(observation.parentSpanId());
        sagaEvent.setTraceId(observation.traceId());
        // Which record carried it, so a later sighting of the same event id can say
        // whether the transport replayed one record or the source published two.
        sagaEvent.setTransportRef(observation.transportRef());
        return sagaEvent;
    }

    private boolean isCompensationStepEvent(WorkflowSpec workflow, String eventType) {
        if (workflow.getCompensationSteps() == null) return false;
        return workflow.getCompensationSteps().stream()
                .filter(step -> step.getEvents() != null)
                .anyMatch(step -> step.getEvents().containsValue(eventType));
    }

    /**
     * Takes the workflow this observation identifies, when the execution had none.
     *
     * <h2>Why an execution can be running without a known workflow</h2>
     * Which workflow a run belongs to is inferred from its events, and the first event
     * observed is not always the first event that happened. One execution's events are
     * spread over several topics and read by several threads, so an execution can be
     * created by {@code INVENTORY_FAILED} -- an event no specification declares as a start
     * -- and be filed as {@code UNRESOLVED}. Its {@code ORDER_CREATED} then arrives a
     * moment later and says exactly which workflow it is.
     *
     * <p>The workflow type used to be written only when the execution row was created, so
     * that answer was computed and thrown away. The run stayed {@code UNRESOLVED} for good:
     * every specification-driven detector abstained, a compensation never reached
     * {@code COMPENSATED}, and the platform reported {@code SAGA_STUCK} and
     * {@code ORPHAN_EVENT} against a system that had done nothing wrong. Those findings
     * described the observer's own read order and blamed the observed for it.
     *
     * <p>So the workflow type is an inference later evidence can improve, not a fact fixed
     * at creation.
     *
     * <h2>What this deliberately will not do</h2>
     * It only ever fills a blank. An execution that already resolved keeps its workflow: a
     * later observation that happens to be ambiguous must not undo what an earlier, clearer
     * one established, or a run would flicker between workflows with the arrival order of
     * its events.
     *
     * <p>A positive disagreement is recorded separately by processEvent. The original
     * workflow label remains available for inspection, but no longer authorises inference.
     *
     * <p>It reports nothing back. It used to return whether this arrival was the one that
     * resolved the workflow, because that arrival was the single case where status was
     * reconstructed from the whole sequence instead of advanced by one event. Every event
     * is reconstructed from the whole sequence now, so the moment of resolution is no
     * longer special and nothing downstream has to be told about it.
     */
    private void adoptWorkflowIfNowKnown(SagaInstance saga, WorkflowSpec workflow, String eventType) {
        if (!SagaInstance.UNRESOLVED.equals(saga.getWorkflowType())) {
            return;
        }

        if (workflow == null) {
            return;
        }

        saga.setWorkflowType(workflow.getName());
        log.info("Execution {} resolved to workflow {} on event {}, which arrived after the "
                        + "execution had been created by an earlier-read event",
                saga.getSagaId(), workflow.getName(), eventType);

        // Findings raised while the workflow was unknown asserted absences that were only
        // true of a run nothing could be judged against. Withdrawn rather than deleted; the
        // scheduled passes raise them again if they still hold now that the run can be read
        // against a specification.
        int withdrawn = anomalyDetectionService.retractAbsenceClaims(saga.getSagaId(),
                "workflow " + workflow.getName() + " was resolved by event " + eventType
                        + ", which arrived after this finding was made");
        if (withdrawn > 0) {
            log.info("Withdrew {} finding(s) made against execution {} while its workflow "
                    + "was unknown", withdrawn, saga.getSagaId());
        }
    }

}
