package com.example.correlation.repository;

import com.example.correlation.domain.ServiceLifecycleEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest(showSql = false, properties = {
        "spring.datasource.url=jdbc:h2:mem:lifecycle_retention;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false",
        "spring.sql.init.mode=never"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class ServiceLifecycleRetentionTest {
    private static final Instant CUTOFF = Instant.parse("2026-09-01T00:00:00Z");

    @Autowired private ServiceLifecycleEventRepository repository;

    @BeforeEach
    void clean() {
        repository.deleteAll();
    }

    @Test
    void oldFactsGoButEachServicesCurrentStateStays() {
        save("inventory-service", ServiceLifecycleEvent.State.DOWN, "2026-08-01T00:00:00Z");
        save("inventory-service", ServiceLifecycleEvent.State.UP, "2026-08-01T00:01:00Z");
        save("inventory-service", ServiceLifecycleEvent.State.DOWN, "2026-09-10T00:00:00Z");

        save("payment-service", ServiceLifecycleEvent.State.UP, "2026-07-01T00:00:00Z");
        save("payment-service", ServiceLifecycleEvent.State.DOWN, "2026-08-15T00:00:00Z");

        int deleted = transactional(() -> repository.deleteExpiredKeepingLatestPerService(CUTOFF));

        assertThat(deleted).isEqualTo(3);
        assertThat(repository.findAll())
                .extracting(e -> e.getServiceName() + ":" + e.getState() + ":" + e.getOccurredAt())
                .containsExactlyInAnyOrder(
                        "inventory-service:DOWN:2026-09-10T00:00:00Z",
                        "payment-service:DOWN:2026-08-15T00:00:00Z");
    }

    @Autowired private org.springframework.transaction.PlatformTransactionManager txManager;

    private int transactional(java.util.function.IntSupplier work) {
        return new org.springframework.transaction.support.TransactionTemplate(txManager)
                .execute(status -> work.getAsInt());
    }

    private void save(String service, ServiceLifecycleEvent.State state, String at) {
        repository.save(new ServiceLifecycleEvent(service, state, Instant.parse(at), "docker", "test",
                service + ":" + state + ":" + at));
    }
}
