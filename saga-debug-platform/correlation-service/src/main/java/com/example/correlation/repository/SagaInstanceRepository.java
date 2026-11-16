package com.example.correlation.repository;

import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, String> {
    List<SagaInstance> findByStatus(SagaStatus status);
    List<SagaInstance> findByStatusAndLastUpdatedBefore(SagaStatus status, Instant before);
    long countByStartTimeAfter(Instant startTime);
    long countByStatusAndStartTimeAfter(SagaStatus status, Instant startTime);

    @Query(value = "SELECT count(*) FROM saga_instances WHERE status = :status "
            + "AND start_time > :since AND CAST(workflow_conflicts AS varchar) = '[]'", nativeQuery = true)
    long countUncontestedByStatusSince(@Param("status") String status, @Param("since") Instant since);
    Page<SagaInstance> findByStatus(SagaStatus status, Pageable pageable);
    Page<SagaInstance> findByStartTimeBetween(Instant start, Instant end, Pageable pageable);
    Page<SagaInstance> findByStatusAndStartTimeBetween(SagaStatus status, Instant start, Instant end, Pageable pageable);

    List<SagaInstance> findByStatusIn(Collection<SagaStatus> statuses, Pageable pageable);

    List<SagaInstance> findByStatusInAndLastUpdatedAfter(
            Collection<SagaStatus> statuses, Instant after, Pageable pageable);

    List<SagaInstance> findByStatusInAndEndTimeAfter(
            Collection<SagaStatus> statuses, Instant after, Pageable pageable);

    List<SagaInstance> findByStatusInAndLastUpdatedBefore(
            Collection<SagaStatus> statuses, Instant before, Pageable pageable);

    @Query("""
            SELECT s.sagaId FROM SagaInstance s
            WHERE s.status IN :statuses
              AND NOT EXISTS (
                  SELECT r.id FROM RcaResult r
                  WHERE r.sagaId = s.sagaId
                    AND r.createdAt >= (SELECT MAX(e.receivedAt) FROM SagaEvent e
                                        WHERE e.sagaId = s.sagaId))
            ORDER BY s.lastUpdated DESC""")
    List<String> findSagaIdsAwaitingAnalysis(
            @Param("statuses") Collection<SagaStatus> statuses, Pageable pageable);

    @Query("""
            SELECT s.sagaId FROM SagaInstance s
            WHERE s.status IN :statuses
              AND EXISTS (SELECT a.id FROM Anomaly a WHERE a.sagaId = s.sagaId)
              AND NOT EXISTS (
                  SELECT r.id FROM RcaResult r
                  WHERE r.sagaId = s.sagaId
                    AND r.createdAt >= (SELECT MAX(e.receivedAt) FROM SagaEvent e
                                        WHERE e.sagaId = s.sagaId))
            ORDER BY s.lastUpdated DESC""")
    List<String> findSagaIdsWithFindingsAwaitingAnalysis(
            @Param("statuses") Collection<SagaStatus> statuses, Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaInstance s WHERE s.sagaId = :sagaId")
    Optional<SagaInstance> findByIdForUpdate(@Param("sagaId") String sagaId);
}
