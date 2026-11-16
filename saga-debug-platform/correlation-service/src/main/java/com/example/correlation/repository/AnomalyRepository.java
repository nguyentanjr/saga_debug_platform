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
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Anomaly x SET x.sagaId = :canonical WHERE x.sagaId = :merged")
    int repointSaga(@Param("merged") String merged, @Param("canonical") String canonical);

    List<Anomaly> findBySagaId(String sagaId);

    @Query("SELECT a FROM Anomaly a WHERE a.sagaId = :sagaId AND a.retractedAt IS NULL")
    List<Anomaly> findStandingBySagaId(@Param("sagaId") String sagaId);
    List<Anomaly> findByDetectedAtAfter(Instant since);

    @Query("SELECT COUNT(a) > 0 FROM Anomaly a WHERE a.sagaId = :sagaId "
            + "AND a.anomalyType = :anomalyType AND a.retractedAt IS NULL")
    boolean existsBySagaIdAndAnomalyType(@Param("sagaId") String sagaId,
            @Param("anomalyType") AnomalyType anomalyType);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Anomaly a SET a.retractedAt = :at, a.retractionReason = :reason "
            + "WHERE a.sagaId = :sagaId AND a.anomalyType IN :types AND a.retractedAt IS NULL")
    int retractClaims(@Param("sagaId") String sagaId,
            @Param("types") Collection<AnomalyType> types,
            @Param("at") Instant at,
            @Param("reason") String reason);
    int deleteByDetectedAtBefore(Instant cutoff);

    @Query("SELECT DISTINCT a.sagaId FROM Anomaly a WHERE a.detectedAt >= :since "
            + "OR a.retractedAt >= :since")
    List<String> findSagaIdsWithAnomaliesSince(@Param("since") Instant since);

    @Query("SELECT MAX(CASE WHEN a.retractedAt IS NOT NULL AND a.retractedAt > a.detectedAt "
            + "THEN a.retractedAt ELSE a.detectedAt END) FROM Anomaly a WHERE a.sagaId = :sagaId")
    Instant findLatestDetectedAtBySagaId(@Param("sagaId") String sagaId);

    @Query("SELECT DISTINCT a.sagaId FROM Anomaly a "
            + "WHERE a.anomalyType = :type AND a.sagaId IN :sagaIds "
            + "AND a.retractedAt IS NULL")
    List<String> findSagaIdsWithAnomalyType(@Param("type") AnomalyType type,
            @Param("sagaIds") Collection<String> sagaIds);

    List<Anomaly> findBySagaIdIn(Collection<String> sagaIds);

    void deleteBySagaIdIn(Collection<String> sagaIds);
}
