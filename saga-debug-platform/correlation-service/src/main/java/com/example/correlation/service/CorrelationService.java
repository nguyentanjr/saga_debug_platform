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

        Optional<SagaEvent> alreadyStored = sagaEventRepository.findBySourceNameAndEventId(
                observation.sourceName(), eventId);
        if (alreadyStored.isPresent()) {
            handleAlreadyStored(observation, alreadyStored.get());
            return;
        }

        SagaInstance saga = sagaInstanceRepository.findByIdForUpdate(sagaId).orElse(null);

        List<SagaEvent> priorEvents = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);

        WorkflowSpec workflow = workflowRegistry
                .resolve(saga != null ? saga.getWorkflowType() : null,
                        observation.declaredWorkflow(),
                        observation.sourceName(),
                        eventType)
                .orElse(null);

        if (saga == null) {
            saga = newSagaInstance(observation, workflow);
        } else {
            adoptWorkflowIfNowKnown(saga, workflow, eventType);
        }

        recordWorkflowConflicts(saga, observation);
        if (saga.hasWorkflowConflict()) {
            log.error("Execution {} has conflicting workflows {}; withholding workflow conclusions",
                    sagaId, saga.getWorkflowConflicts());
            workflow = null;
        }

        SagaEvent sagaEvent = observedEventOf(observation);

        List<SagaEvent> allEvents = new ArrayList<>(priorEvents);
        allEvents.add(sagaEvent);

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, allEvents);
        SagaStatus newStatus = timeline.status();
        saga.setStatus(newStatus);

        widenTimeRange(saga, timeline, eventTimestamp);
        saga.setEndTime(timeline.endedAt().orElse(null));

        sagaInstanceRepository.save(saga);
        sagaEventRepository.save(sagaEvent);

        log.info("Saga {} | event={} | step={} | status={}", sagaId, eventType, stepName, newStatus);

        if (saga.hasWorkflowConflict()) return;

        runDetectors(workflow, sagaId, newStatus, timeline, allEvents);
    }

    private void runDetectors(WorkflowSpec workflow, String sagaId, SagaStatus newStatus,
            EvidenceTimeline timeline, List<SagaEvent> allEvents) {
        if (timeline.endedAt().isPresent()) {
            anomalyDetectionService.withdrawMissingTerminal(sagaId, newStatus);
        }

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

        anomalyDetectionService.detectOutOfOrder(sagaId, allEvents);
        anomalyDetectionService.detectRetryBurst(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectMultiTerminalConflict(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectDuplicateCompensation(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectMissingTraceContext(sagaId, sagaEvents);

        anomalyDetectionService.detectOrphanEvent(workflow, sagaId, sagaEvents);
        anomalyDetectionService.withdrawAcknowledgedCompensation(workflow, sagaId, sagaEvents);
        anomalyDetectionService.withdrawStartedCompensation(workflow, sagaId, sagaEvents);
        anomalyDetectionService.withdrawDisprovenSilence(workflow, sagaId, sagaEvents);

        judgeTransitions(workflow, sagaId, timeline);
    }

    private void handleAlreadyStored(IngestedEvent observation, SagaEvent stored) {
        String eventId = observation.eventId();
        String sagaId = observation.sagaId();
        String storedOn = stored.getTransportRef();
        String arrivedOn = observation.transportRef();

        if (storedOn != null && storedOn.equals(arrivedOn)) {
            log.info("Re-read of stored event {} for saga {} on the same record {}; "
                            + "observer-side replay, not a duplicate delivery.",
                    eventId, sagaId, storedOn);
            return;
        }

        log.info("Duplicate event {} for saga {} (DB hit). Skipping.", eventId, sagaId);
        anomalyDetectionService.detectDuplicateEvent(sagaId, eventId, observation.eventType(),
                storedOn, arrivedOn);
    }

    private static SagaInstance newSagaInstance(IngestedEvent observation, WorkflowSpec workflow) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId(observation.sagaId());
        saga.setSourceName(observation.sourceName());
        saga.setStatus(SagaStatus.STARTED);
        saga.setStartTime(observation.eventTime());
        saga.setLastUpdated(observation.eventTime());
        if (workflow != null) {
            saga.setWorkflowType(workflow.getName());
        }
        saga.setOrderId(observation.businessKey());
        log.info("New saga detected: {} (workflow={})", observation.sagaId(), saga.getWorkflowType());
        return saga;
    }

    private void recordWorkflowConflicts(SagaInstance saga, IngestedEvent observation) {
        saga.recordWorkflowConflict(workflowRegistry.resolveFromEvidence(
                observation.declaredWorkflow(), observation.sourceName(), observation.eventType())
                .map(WorkflowSpec::getName).orElse(null));
        saga.recordWorkflowConflict(workflowRegistry.byType(observation.declaredWorkflow())
                .map(WorkflowSpec::getName).orElse(null));
    }

    private static void widenTimeRange(SagaInstance saga, EvidenceTimeline timeline, Instant fallback) {
        Instant earliest = timeline.startedAt().orElse(fallback);
        if (saga.getStartTime() == null || earliest.isBefore(saga.getStartTime())) {
            saga.setStartTime(earliest);
        }
        Instant latest = timeline.lastEventAt().orElse(fallback);
        if (saga.getLastUpdated() == null || latest.isAfter(saga.getLastUpdated())) {
            saga.setLastUpdated(latest);
        }
    }

    private void judgeTransitions(WorkflowSpec workflow, String sagaId, EvidenceTimeline timeline) {
        if (workflow == null) return;

        List<SagaEvent> inOrder = timeline.inBusinessOrder();
        Set<String> invalid = new HashSet<>();
        for (int i = 1; i < inOrder.size(); i++) {
            SagaEvent previous = inOrder.get(i - 1);
            SagaEvent current = inOrder.get(i);
            if (isCompensationStepEvent(workflow, current.getEventType())) continue;

            if (timeline.isAfterEnding(current)) continue;

            if (timeline.isSupersededByCompensation(current)
                    || timeline.isSupersededByCompensation(previous)) continue;

            String previousStepName = workflow.getExpectedStepForEvent(previous.getEventType());
            String currentStepName = workflow.getExpectedStepForEvent(current.getEventType());
            if (previousStepName == null || currentStepName == null) continue;

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

    private Optional<String> handoffCondition(WorkflowSpec workflow, String eventType) {
        return workflow.classifyEvent(eventType)
                .filter(role -> role.role() == WorkflowSpec.EventRole.Role.COMPLETED
                        || role.role() == WorkflowSpec.EventRole.Role.FAILED)

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

        sagaEvent.setTransportRef(observation.transportRef());
        return sagaEvent;
    }

    private boolean isCompensationStepEvent(WorkflowSpec workflow, String eventType) {
        if (workflow.getCompensationSteps() == null) return false;
        return workflow.getCompensationSteps().stream()
                .filter(step -> step.getEvents() != null)
                .anyMatch(step -> step.getEvents().containsValue(eventType));
    }

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

        int withdrawn = anomalyDetectionService.retractAbsenceClaims(saga.getSagaId(),
                "workflow " + workflow.getName() + " was resolved by event " + eventType
                        + ", which arrived after this finding was made");
        if (withdrawn > 0) {
            log.info("Withdrew {} finding(s) made against execution {} while its workflow "
                    + "was unknown", withdrawn, saga.getSagaId());
        }
    }
}
