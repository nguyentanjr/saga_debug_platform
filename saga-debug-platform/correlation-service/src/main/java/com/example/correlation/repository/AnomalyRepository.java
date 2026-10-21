package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface AnomalyRepository extends JpaRepository<Anomaly, Long> {
    /**
     * Files every observation of one execution under another identifier.
     *
     * <p>Used when two identifiers turn out to name the same execution: see
     * {@code ExecutionAliasResolver}. A bulk update rather than a load-and-save because
     * the losing chain can be any length and none of it needs to pass through memory to
     * be renamed.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Anomaly x SET x.sagaId = :canonical WHERE x.sagaId = :merged")
    int repointSaga(@Param("merged") String merged, @Param("canonical") String canonical);

    /**
     * Every finding ever raised about this execution, withdrawn ones included.
     *
     * <p>For showing an operator the history. A caller that is going to <em>reason</em>
     * from these wants {@link #findStandingBySagaId} instead: a withdrawn row is a claim
     * the platform has said it no longer stands behind, and citing it is worse than never
     * having withdrawn it, because the report reads as current.
     */
    List<Anomaly> findBySagaId(String sagaId);

    /**
     * The findings that still stand against this execution.
     *
     * <p>What root-cause analysis reasons from. It used to read {@link #findBySagaId}, so a
     * run whose SAGA_STUCK was withdrawn the moment its late events arrived was still
     * diagnosed as a crashed service, citing a stuck period the platform had already
     * retracted.
     */
    @Query("SELECT a FROM Anomaly a WHERE a.sagaId = :sagaId AND a.retractedAt IS NULL")
    List<Anomaly> findStandingBySagaId(@Param("sagaId") String sagaId);
    List<Anomaly> findByDetectedAtAfter(Instant since);
    /**
     * Whether a finding of this type already stands against this execution.
     *
     * <p>Every detector asks this before raising, so it decides whether a finding is
     * raised once or repeatedly. Retracted rows are deliberately invisible here: a
     * retraction says "later observations falsified this", and the next detector pass has
     * to be free to raise it again if it is still true of the fuller picture. Counting a
     * retracted row would suppress exactly the re-check that makes retraction safe.
     */
    @Query("SELECT COUNT(a) > 0 FROM Anomaly a WHERE a.sagaId = :sagaId "
            + "AND a.anomalyType = :anomalyType AND a.retractedAt IS NULL")
    boolean existsBySagaIdAndAnomalyType(@Param("sagaId") String sagaId,
            @Param("anomalyType") AnomalyType anomalyType);

    /**
     * Withdraws the findings about one execution that later observations may have
     * falsified, leaving the rows in place with the reason.
     *
     * @param types the types whose claim is an absence; an occurrence cannot be undone by
     *              observing more
     * @return how many findings were withdrawn
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Anomaly a SET a.retractedAt = :at, a.retractionReason = :reason "
            + "WHERE a.sagaId = :sagaId AND a.anomalyType IN :types AND a.retractedAt IS NULL")
    int retractClaims(@Param("sagaId") String sagaId,
            @Param("types") Collection<AnomalyType> types,
            @Param("at") Instant at,
            @Param("reason") String reason);
    int deleteByDetectedAtBefore(Instant cutoff);

    /**
     * Runs whose findings changed since the instant: raised, or withdrawn. A withdrawal changes
     * the evidence a diagnosis rests on as much as a new finding does; counting only raises left
     * a diagnosis made while ORPHAN_EVENT stood, and written just after it was withdrawn,
     * standing for good (EXT-OVERLOAD-FALSE-FINDINGS, bench 2000 ev/s).
     */
    @Query("SELECT DISTINCT a.sagaId FROM Anomaly a WHERE a.detectedAt >= :since "
            + "OR a.retractedAt >= :since")
    List<String> findSagaIdsWithAnomaliesSince(@Param("since") Instant since);

    /** When this run's findings last changed: the latest raise or withdrawal. */
    @Query("SELECT MAX(CASE WHEN a.retractedAt IS NOT NULL AND a.retractedAt > a.detectedAt "
            + "THEN a.retractedAt ELSE a.detectedAt END) FROM Anomaly a WHERE a.sagaId = :sagaId")
    Instant findLatestDetectedAtBySagaId(@Param("sagaId") String sagaId);

    /**
     * Which of these sagas already carry this anomaly type.
     *
     * <p>Replaces one existsBySagaIdAndAnomalyType call per saga inside the scheduler
     * loops: the same answer for a whole batch in a single round trip.
     *
     * <p>Which means it has to answer the same question, retraction filter included. It did
     * not: a withdrawn row counted here, so a retracted finding would have suppressed for
     * good the re-check that makes retraction safe. Unreachable while the only caller
     * passes an occurrence type, and wrong the moment one passes an absence type.
     */
    @Query("SELECT DISTINCT a.sagaId FROM Anomaly a "
            + "WHERE a.anomalyType = :type AND a.sagaId IN :sagaIds "
            + "AND a.retractedAt IS NULL")
    List<String> findSagaIdsWithAnomalyType(@Param("type") AnomalyType type,
            @Param("sagaIds") Collection<String> sagaIds);

    List<Anomaly> findBySagaIdIn(Collection<String> sagaIds);

    /** Bulk removal used by retention; children are deleted before their saga. */
    void deleteBySagaIdIn(Collection<String> sagaIds);
}
