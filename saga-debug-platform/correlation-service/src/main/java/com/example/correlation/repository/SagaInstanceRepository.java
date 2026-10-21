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

    /** A retained status is not a conclusion while its workflow is disputed. */
    @Query(value = "SELECT count(*) FROM saga_instances WHERE status = :status "
            + "AND start_time > :since AND CAST(workflow_conflicts AS varchar) = '[]'", nativeQuery = true)
    long countUncontestedByStatusSince(@Param("status") String status, @Param("since") Instant since);
    Page<SagaInstance> findByStatus(SagaStatus status, Pageable pageable);
    Page<SagaInstance> findByStartTimeBetween(Instant start, Instant end, Pageable pageable);
    Page<SagaInstance> findByStatusAndStartTimeBetween(SagaStatus status, Instant start, Instant end, Pageable pageable);

    // ── Scheduled-detector access paths ──────────────────────────────────────
    //
    // The detectors previously called findByStatus(COMPLETED) and filtered the
    // recency window in Java, which loads every saga ever completed into heap on
    // every pass — a cost that grows with total history rather than with the work
    // there is to do. These push both the time bound and the row limit into the
    // query, so a pass costs the same whether the table holds a thousand rows or
    // a hundred million.

    /** Sagas in any of the given states, most recently updated first. */
    List<SagaInstance> findByStatusIn(Collection<SagaStatus> statuses, Pageable pageable);

    /** Sagas in any of the given states whose last update falls inside the window. */
    List<SagaInstance> findByStatusInAndLastUpdatedAfter(
            Collection<SagaStatus> statuses, Instant after, Pageable pageable);

    /** Sagas in any of the given states that ended inside the window. */
    List<SagaInstance> findByStatusInAndEndTimeAfter(
            Collection<SagaStatus> statuses, Instant after, Pageable pageable);

    /** Sagas in any of the given states left untouched since the cutoff. */
    List<SagaInstance> findByStatusInAndLastUpdatedBefore(
            Collection<SagaStatus> statuses, Instant before, Pageable pageable);

    /**
     * Sagas in any of the given states holding an observation received after their newest
     * analysis, or never analysed. Compared on received_at, the platform's own clock:
     * last_updated is the source's clock, and a run read from backlog after the source
     * stamped it would fall outside any recency window on it.
     */
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

    /**
     * The same staleness test, restricted to runs that carry findings. For completed runs:
     * a run diagnosed while it waited and later finished holds a diagnosis made on part of
     * its evidence, and finishing need not raise any new finding to trigger the finding-
     * driven pass. Runs without findings are left out, so healthy completions are not
     * analysed for nothing.
     */
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

    /**
     * Locks the saga row for the duration of the caller's transaction.
     *
     * <p>RCA analysis deletes the previous result and writes a new one. Two analyses of
     * the same saga can overlap — the 30s auto-sweep and a manual POST /rca/analyze,
     * which is exactly what the evaluation script does — and without serialization both
     * delete, then both insert, leaving two results where the code assumes one.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaInstance s WHERE s.sagaId = :sagaId")
    Optional<SagaInstance> findByIdForUpdate(@Param("sagaId") String sagaId);
}
