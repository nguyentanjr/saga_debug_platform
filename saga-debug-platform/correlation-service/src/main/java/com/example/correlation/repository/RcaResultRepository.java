package com.example.correlation.repository;

import com.example.correlation.domain.RcaResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface RcaResultRepository extends JpaRepository<RcaResult, Long> {
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE RcaResult x SET x.sagaId = :canonical WHERE x.sagaId = :merged")
    int repointSaga(@Param("merged") String merged, @Param("canonical") String canonical);

    List<RcaResult> findBySagaIdOrderByCreatedAtDesc(String sagaId);
    List<RcaResult> findBySagaIdInOrderByCreatedAtDesc(Collection<String> sagaIds);
    List<RcaResult> findByCreatedAtAfter(Instant since);
    void deleteAllBySagaId(String sagaId);

    void deleteBySagaIdIn(Collection<String> sagaIds);
}
