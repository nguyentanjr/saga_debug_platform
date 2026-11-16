package com.example.correlation.repository;

import com.example.correlation.domain.SagaEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface SagaEventRepository extends JpaRepository<SagaEvent, Long> {
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE SagaEvent x SET x.sagaId = :canonical WHERE x.sagaId = :merged")
    int repointSaga(@Param("merged") String merged, @Param("canonical") String canonical);

    List<SagaEvent> findBySagaIdOrderByTimestampAsc(String sagaId);
    List<SagaEvent> findBySagaIdOrderByReceivedAtAsc(String sagaId);

    Optional<SagaEvent> findBySourceNameAndEventId(String sourceName, String eventId);
    long countBySagaId(String sagaId);
    int deleteByTimestampBefore(Instant cutoff);
    Optional<SagaEvent> findTopBySagaIdOrderByTimestampDesc(String sagaId);

    @Query("SELECT e.sagaId, COUNT(e) FROM SagaEvent e WHERE e.sagaId IN :sagaIds GROUP BY e.sagaId")
    List<Object[]> countBySagaIdIn(@Param("sagaIds") List<String> sagaIds);

    List<SagaEvent> findBySagaIdInOrderBySagaIdAscTimestampAsc(Collection<String> sagaIds);

    void deleteBySagaIdIn(Collection<String> sagaIds);
}
