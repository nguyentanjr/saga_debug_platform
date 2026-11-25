package com.example.correlation.service;

import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
@ConditionalOnProperty(prefix = "retention", name = "enabled", havingValue = "true")
public class RetentionService {
    private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

    private static final List<SagaStatus> TERMINAL_STATUSES =
            List.of(SagaStatus.COMPLETED, SagaStatus.FAILED, SagaStatus.COMPENSATED);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final RetentionProperties properties;
    private final ServiceLifecycleEventRepository lifecycleRepository;

    public RetentionService(SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository,
            RetentionProperties properties,
            ServiceLifecycleEventRepository lifecycleRepository) {
        this.lifecycleRepository = lifecycleRepository;
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.properties = properties;
    }

    @Transactional
    public void purgeExpiredSagas() {
        Instant cutoff = Instant.now().minus(properties.getMaxAge());
        int purged = purgeOlderThan(cutoff);
        if (purged > 0) {
            log.info("Retention purge removed {} saga(s) settled before {}", purged, cutoff);
        }

        int lifecycle = lifecycleRepository.deleteExpiredKeepingLatestPerService(cutoff);
        if (lifecycle > 0) {
            log.info("Retention purge removed {} service lifecycle fact(s) before {}", lifecycle, cutoff);
        }
    }

    int purgeOlderThan(Instant cutoff) {
        List<String> expired = sagaInstanceRepository
                .findByStatusInAndLastUpdatedBefore(TERMINAL_STATUSES, cutoff,
                        PageRequest.of(0, properties.getBatchSize(),
                                Sort.by(Sort.Direction.ASC, "lastUpdated")))
                .stream()
                .map(SagaInstance::getSagaId)
                .toList();

        if (expired.isEmpty()) {
            return 0;
        }

        rcaResultRepository.deleteBySagaIdIn(expired);
        anomalyRepository.deleteBySagaIdIn(expired);
        sagaEventRepository.deleteBySagaIdIn(expired);
        sagaInstanceRepository.deleteAllById(expired);

        return expired.size();
    }
}
