package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Values the observed system chooses must fit the columns that store them (EXT-LONG-TYPE).
 *
 * <p>An event type no mapping translates is stored as the source wrote it. For a Java system
 * that is a fully qualified class name; FTGO's OrderCreatedEvent is 66 characters and did not
 * fit VARCHAR(50), so the record failed, was retried, and held up its partition.
 *
 * <p>Runs on the H2 schema that stands in for the migrations. Set
 * {@code -Dsvw.jdbc.url=jdbc:postgresql://... -Dsvw.flyway=true -Dsvw.init=never}
 * (plus driver/username/password) to run the same test against Postgres with the real
 * Flyway migrations.
 */
@DataJpaTest(showSql = false, properties = {
        "spring.datasource.url=${svw.jdbc.url:jdbc:h2:mem:svw;MODE=PostgreSQL;DB_CLOSE_DELAY=-1}",
        "spring.datasource.driver-class-name=${svw.jdbc.driver:org.h2.Driver}",
        "spring.datasource.username=${svw.jdbc.username:sa}",
        "spring.datasource.password=${svw.jdbc.password:}",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.flyway.enabled=${svw.flyway:false}",
        "spring.sql.init.mode=${svw.init:always}",
        "spring.sql.init.schema-locations=classpath:schema-h2.sql"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class SourceValueWidthTest {

    private static final Instant AT = Instant.parse("2026-10-16T08:04:20Z");
    private static final String FTGO_TYPE =
            "net.chrisrichardson.ftgo.orderservice.api.events.OrderCreatedEvent";

    @Autowired private SagaEventRepository events;
    @Autowired private SagaInstanceRepository instances;
    @Autowired private PlatformTransactionManager transactions;

    @Test
    void anUntranslatedFullyQualifiedEventTypeIsStoredAsTheSourceWroteIt() {
        String run = UUID.randomUUID().toString();
        String step = "step.".repeat(30);                       // 150 characters
        String service = "svc-".repeat(40);                     // 160 characters
        CorrelationService correlation = new CorrelationService(instances, events,
                mock(AnomalyDetectionService.class), new DeduplicationCache(),
                loadedWorkflows(), new ObjectMapper().findAndRegisterModules());

        new TransactionTemplate(transactions).executeWithoutResult(status ->
                correlation.processEvent(IngestedEvent.withoutCausality("e-" + run, run,
                        FTGO_TYPE, step, service, AT, "{}", AT)));

        var stored = events.findBySagaIdOrderByTimestampAsc("default::" + run);
        assertThat(stored).hasSize(1);
        assertThat(stored.get(0).getEventType()).isEqualTo(FTGO_TYPE);
        assertThat(stored.get(0).getStepName()).isEqualTo(step);
        assertThat(stored.get(0).getServiceName()).isEqualTo(service);
    }

    private static WorkflowRegistry loadedWorkflows() {
        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        return workflows;
    }
}
