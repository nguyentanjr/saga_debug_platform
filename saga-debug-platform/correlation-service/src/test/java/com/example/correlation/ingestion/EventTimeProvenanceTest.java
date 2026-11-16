package com.example.correlation.ingestion;

import com.example.correlation.domain.*;
import com.example.correlation.dto.SagaDetailResponse;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@DataJpaTest(showSql = false, properties = {
        "spring.datasource.url=${f6.jdbc.url:jdbc:h2:mem:f6;MODE=PostgreSQL;DB_CLOSE_DELAY=-1}",
        "spring.datasource.driver-class-name=${f6.jdbc.driver:org.h2.Driver}",
        "spring.datasource.username=${f6.jdbc.username:sa}",
        "spring.datasource.password=${f6.jdbc.password:}",
        "spring.jpa.hibernate.ddl-auto=${f6.ddl:create-drop}",
        "spring.flyway.enabled=${f6.flyway:false}",
        "spring.sql.init.mode=never"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class EventTimeProvenanceTest {
    private static final Instant EVENT = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant TRANSPORT = EVENT.plusSeconds(60);

    @Autowired private SagaEventRepository events;
    @Autowired private SagaInstanceRepository instances;
    @Autowired private PlatformTransactionManager transactions;
    @Autowired private JdbcTemplate jdbc;

    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
    private String run;
    private SourceMapping mapping;
    private SourceMappingRegistry mappings;
    private AnomalyDetectionService detector;
    private CorrelationService correlation;
    private EventIngestionService ingestion;

    @BeforeEach
    void setUp() {
        run = UUID.randomUUID().toString();
        mapping = SourceMapping.identity();
        mappings = mock(SourceMappingRegistry.class);
        when(mappings.forTopic(any(), anyString())).thenReturn(mapping);
        detector = mock(AnomalyDetectionService.class);
        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        correlation = new CorrelationService(instances, events, detector, new DeduplicationCache(), workflows, json);
        ingestion = newIngestion();
    }

    @Test
    void declaredTimeAndProvenanceSurviveReloadAndAppearTogetherInTheReadApi() throws Exception {
        ingest("e1", "\"2026-01-01T00:00:00Z\"");
        SagaEvent stored = stored("e1");
        assertThat(stored.getTimestamp()).isEqualTo(EVENT);
        assertThat(stored.getEventTimeSource()).isEqualTo(EventTimeSource.DECLARED);
        SagaDetailResponse response = new SagaDetailResponse();
        response.setEvents(List.of(stored));
        assertThat(json.readTree(json.writeValueAsString(response)).at("/events/0/eventTimeSource").asText())
                .isEqualTo("DECLARED");
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "\"\"", "\"not-a-time\"", "{}"})
    void unusableDeclaredTimePersistsTransportProvenance(String value) {
        ingest("e1", value);
        assertThat(stored("e1").getTimestamp()).isEqualTo(TRANSPORT);
        assertThat(stored("e1").getEventTimeSource()).isEqualTo(EventTimeSource.TRANSPORT);
    }

    @Test
    void aMissingTimestampPersistsTransportProvenance() {
        ingestion.ingest(raw("e1", null, TRANSPORT, Map.of()));
        assertThat(stored("e1").getTimestamp()).isEqualTo(TRANSPORT);
        assertThat(stored("e1").getEventTimeSource()).isEqualTo(EventTimeSource.TRANSPORT);
    }

    @Test
    void mappedHeaderTimeIsDeclaredEvenWhenItEqualsTransportTime() {
        mapping.getFields().setEventTime(TextNode.valueOf("header:occurred-at"));
        ingestion.ingest(raw("e1", null, TRANSPORT, Map.of("occurred-at", TRANSPORT.toString())));
        assertThat(stored("e1").getTimestamp()).isEqualTo(TRANSPORT);
        assertThat(stored("e1").getEventTimeSource()).isEqualTo(EventTimeSource.DECLARED);
    }

    @Test
    void lateEventsKeepTheirOwnProvenanceRatherThanTheExecutionsLatestClock() {
        ingest("e2", null);
        ingest("e1", "\"2026-01-01T00:00:00Z\"");
        List<SagaEvent> reloaded = events.findBySagaIdOrderByTimestampAsc("default::" + run);
        assertThat(reloaded).extracting(SagaEvent::getEventTimeSource)
                .containsExactly(EventTimeSource.DECLARED, EventTimeSource.TRANSPORT);
        assertThat(reloaded).extracting(SagaEvent::getTimestamp).containsExactly(EVENT, TRANSPORT);
    }

    @Test
    void duplicateAndReplayAfterCacheLossDoNotReplaceTheOriginalTimestampOrItsOrigin() {
        RawObservation first = raw("e1", null, TRANSPORT, Map.of());
        ingestion.ingest(first);
        assertThat(ingestion.ingest(first).outcome()).isEqualTo(IngestionResult.Outcome.DUPLICATE);

        mapping.getFields().setEventTime(TextNode.valueOf("header:occurred-at"));
        newIngestion().ingest(raw("e1", null, TRANSPORT.plusSeconds(20),
                Map.of("occurred-at", EVENT.toString())));
        assertThat(events.countBySagaId("default::" + run)).isEqualTo(1);
        assertThat(stored("e1").getTimestamp()).isEqualTo(TRANSPORT);
        assertThat(stored("e1").getEventTimeSource()).isEqualTo(EventTimeSource.TRANSPORT);
    }

    @Test
    void observationAndProvenanceRollbackTogetherAndRetryCanCommit() {
        doThrow(new IllegalStateException("failure after saving observation"))
                .when(detector).detectOutOfOrder(anyString(), any());
        assertThatThrownBy(() -> ingest("e1", "\"2026-01-01T00:00:00Z\""))
                .isInstanceOf(IllegalStateException.class);
        assertThat(events.countBySagaId("default::" + run)).isZero();
        assertThat(instances.findById("default::" + run)).isEmpty();
        reset(detector);
        assertThat(ingest("e1", "\"2026-01-01T00:00:00Z\"").isAccepted()).isTrue();
        assertThat(stored("e1").getEventTimeSource()).isEqualTo(EventTimeSource.DECLARED);
    }

    @Test
    void legacyCallerRemainsUnknownEvenWithATimestampInItsPayload() {
        new TransactionTemplate(transactions).executeWithoutResult(status ->
                correlation.processEvent(IngestedEvent.withoutCausality(id("e1"), run,
                        "ORDER_CREATED", "UNKNOWN", "test", EVENT,
                        "{\"timestamp\":\"2026-01-01T00:00:00Z\"}", EVENT)));
        assertThat(stored("e1").getEventTimeSource()).isNull();
    }

    @Test
    void databaseRejectsAnUnknownNonNullOrigin() {
        ingest("e1", null);
        assertThatThrownBy(() -> jdbc.update(
                "UPDATE saga_events SET event_time_source = 'GUESSED' WHERE event_id = ?", id("e1")))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThat(stored("e1").getEventTimeSource()).isEqualTo(EventTimeSource.TRANSPORT);
    }

    @Test
    void workflowConflictSurvivesReloadReplayAndLaterMatchingEvidence() {
        ingest("e1", null);
        long runningBefore = instances.countUncontestedByStatusSince("IN_PROGRESS", EVENT.minusSeconds(1));
        assertThat(runningBefore).isPositive();
        mapping.setWorkflow("LOAN_APPROVAL");
        ingest("e2", null);
        assertThat(instances.countUncontestedByStatusSince("IN_PROGRESS", EVENT.minusSeconds(1)))
                .isEqualTo(runningBefore - 1);
        assertThat(instances.findById("default::" + run).orElseThrow().getWorkflowConflicts())
                .containsExactly("LOAN_APPROVAL", "ORDER_SAGA");
        mapping.setWorkflow("ORDER_SAGA");
        newIngestion().ingest(raw("e2", null, TRANSPORT, Map.of()));
        newIngestion().ingest(raw("e3", null, TRANSPORT, Map.of()));
        assertThat(instances.findById("default::" + run).orElseThrow().getWorkflowConflicts())
                .containsExactly("LOAN_APPROVAL", "ORDER_SAGA");
        assertThat(events.countBySagaId("default::" + run)).isEqualTo(3);
    }

    @Test
    void workflowConflictRollsBackWithItsObservationAndRetryCanCommit() {
        ingest("e1", null);
        mapping.setWorkflow("LOAN_APPROVAL");
        mapping.getEventTypes().put("ORDER_CREATED", "X".repeat(400));
        assertThatThrownBy(() -> ingest("e2", null))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThat(instances.findById("default::" + run).orElseThrow().hasWorkflowConflict()).isFalse();
        assertThat(events.countBySagaId("default::" + run)).isEqualTo(1);
        mapping.getEventTypes().clear();
        assertThat(ingest("e2", null).isAccepted()).isTrue();
        assertThat(instances.findById("default::" + run).orElseThrow().hasWorkflowConflict()).isTrue();
    }

    @Test
    void aConcurrentStaleWriterCannotClearACommittedWorkflowConflict() throws Exception {
        ingest("e1", null);
        var loaded = new java.util.concurrent.CountDownLatch(1);
        var committed = new java.util.concurrent.CountDownLatch(1);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var staleWrite = executor.submit(() -> new TransactionTemplate(transactions).executeWithoutResult(tx -> {
                SagaInstance stale = instances.findById("default::" + run).orElseThrow();
                loaded.countDown();
                try {
                    if (!committed.await(10, java.util.concurrent.TimeUnit.SECONDS))
                        throw new IllegalStateException("Timed out waiting for conflict commit");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                stale.setLastUpdated(TRANSPORT.plusSeconds(1));
                instances.saveAndFlush(stale);
            }));
            assertThat(loaded.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            mapping.setWorkflow("LOAN_APPROVAL");
            ingest("e2", null);
            committed.countDown();
            assertThatThrownBy(() -> staleWrite.get(10, java.util.concurrent.TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .hasCauseInstanceOf(org.springframework.orm.ObjectOptimisticLockingFailureException.class);
            assertThat(instances.findById("default::" + run).orElseThrow().getWorkflowConflicts())
                    .containsExactly("LOAN_APPROVAL", "ORDER_SAGA");
        } finally {
            committed.countDown();
            executor.shutdownNow();
        }
    }

    private EventIngestionService newIngestion() {
        return new EventIngestionService(correlation, json, new SourceCompletenessCheck(),
                new DeduplicationCache(), new EventTimeResolver(), mappings,
                new PassthroughAliasResolver(), transactions);
    }

    private String id(String name) { return run + "-" + name; }

    private SagaEvent stored(String name) {
        return events.findBySourceNameAndEventId("default", id(name)).orElseThrow();
    }

    private IngestionResult ingest(String name, String timeJson) {
        return ingestion.ingest(raw(name, timeJson, TRANSPORT, Map.of()));
    }

    private RawObservation raw(String name, String timeJson, Instant transport, Map<String, String> headers) {
        String body = "{\"eventId\":\"" + id(name) + "\",\"sagaId\":\"" + run
                + "\",\"eventType\":\"ORDER_CREATED\""
                + (timeJson == null ? "" : ",\"timestamp\":" + timeJson) + "}";
        return new RawObservation("order-created", body, transport, id(name) + "-record", null, headers);
    }
}
