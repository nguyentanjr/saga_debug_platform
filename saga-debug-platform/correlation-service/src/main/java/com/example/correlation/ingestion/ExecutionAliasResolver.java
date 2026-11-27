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

    @Transactional
    public ExecutionKey canonical(ExecutionKey declared, ExecutionKey alias,
            String eventId, Instant observedAt) {
        if (declared == null) {
            return null;
        }
        if (alias == null || alias.equals(declared)) {
            Optional<ExecutionKey> known = lookup(declared);
            if (known.isPresent()) {
                return known.get();
            }

            register(declared, declared, eventId, observedAt);
            return declared;
        }

        ExecutionKey left = lookup(declared).orElse(declared);
        ExecutionKey right = lookup(alias).orElse(alias);
        if (left.equals(right)) {
            return left;
        }

        if (!declared.sameSourceAs(alias)) {
            log.warn("Cross-source link declared: {} and {} are the same run per event {}",
                    declared.canonical(), alias.canonical(), eventId);
        }

        boolean leftHasData = sagaInstances.findById(left.canonical()).isPresent();
        boolean rightHasData = sagaInstances.findById(right.canonical()).isPresent();

        if (leftHasData && rightHasData) {
            return merge(left, right, eventId, observedAt);
        }

        ExecutionKey canonical = rightHasData ? right : left;
        register(left, canonical, eventId, observedAt);
        register(right, canonical, eventId, observedAt);
        return canonical;
    }

    ExecutionKey merge(ExecutionKey requestedLeft, ExecutionKey requestedRight,
            String eventId, Instant observedAt) {
        lockInOrder(requestedLeft, requestedRight);

        ExecutionKey left = lookup(requestedLeft).orElse(requestedLeft);
        ExecutionKey right = lookup(requestedRight).orElse(requestedRight);
        if (left.equals(right)) {
            log.debug("Executions {} and {} were already folded while this worker waited",
                    requestedLeft.canonical(), requestedRight.canonical());
            return left;
        }

        Optional<SagaInstance> leftInstance = sagaInstances.findById(left.canonical());
        Optional<SagaInstance> rightInstance = sagaInstances.findById(right.canonical());

        ExecutionKey canonical = survivorOf(left, leftInstance, right, rightInstance);
        ExecutionKey merged = canonical.equals(left) ? right : left;

        log.info("Executions {} and {} are one run: event {} carried both identifiers. "
                + "Filing everything under {}.",
                left.canonical(), right.canonical(), eventId, canonical.canonical());

        int movedEvents = sagaEvents.repointSaga(merged.canonical(), canonical.canonical());
        int movedAnomalies = anomalies.repointSaga(merged.canonical(), canonical.canonical());
        int movedDiagnoses = rcaResults.repointSaga(merged.canonical(), canonical.canonical());

        int retracted = anomalies.retractClaims(canonical.canonical(), AnomalyType.absenceClaims(),
                Instant.now(),
                "executions " + left.canonical() + " and " + right.canonical()
                        + " were folded into one by event " + eventId
                        + "; this finding was made against a fragment");

        Optional<SagaInstance> survivor = canonical.equals(left) ? leftInstance : rightInstance;
        Optional<SagaInstance> absorbed = canonical.equals(left) ? rightInstance : leftInstance;
        survivor.ifPresent(instance -> absorbed.ifPresent(other -> fold(instance, other)));
        absorbed.ifPresent(sagaInstances::delete);

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

        return left.canonical().compareTo(right.canonical()) <= 0 ? left : right;
    }

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
        if (SagaInstance.UNRESOLVED.equals(survivor.getWorkflowType())
                && !SagaInstance.UNRESOLVED.equals(absorbed.getWorkflowType())) {
            survivor.setWorkflowType(absorbed.getWorkflowType());
        }
        survivor.recordWorkflowConflict(absorbed.getWorkflowType());
        absorbed.getWorkflowConflicts().forEach(survivor::recordWorkflowConflict);
        if (survivor.hasWorkflowConflict()) {
            survivor.setStatus(SagaStatus.IN_PROGRESS);
            survivor.setEndTime(null);
        }
        sagaInstances.save(survivor);
    }

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

    public List<String> identifiersOf(ExecutionKey executionKey) {
        return aliases.findByExecutionId(executionKey.canonical()).stream()
                .map(alias -> ExecutionKey.of(alias.getSourceName(), alias.getAliasKey())
                        .canonical())
                .toList();
    }
}
