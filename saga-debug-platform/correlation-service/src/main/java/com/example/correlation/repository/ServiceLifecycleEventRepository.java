package com.example.correlation.repository;

import com.example.correlation.domain.ServiceLifecycleEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface ServiceLifecycleEventRepository extends JpaRepository<ServiceLifecycleEvent, Long> {
    List<ServiceLifecycleEvent> findByServiceNameInAndOccurredAtBetweenOrderByOccurredAtAsc(
            Collection<String> serviceNames, Instant from, Instant to);

    boolean existsBySourceAndExternalId(String source, String externalId);

    Optional<ServiceLifecycleEvent> findTopBySourceOrderByOccurredAtDesc(String source);

    @Modifying
    @Query("DELETE FROM ServiceLifecycleEvent e WHERE e.occurredAt < :cutoff "
            + "AND e.occurredAt < (SELECT MAX(x.occurredAt) FROM ServiceLifecycleEvent x "
            + "WHERE x.serviceName = e.serviceName)")
    int deleteExpiredKeepingLatestPerService(@Param("cutoff") Instant cutoff);
}
