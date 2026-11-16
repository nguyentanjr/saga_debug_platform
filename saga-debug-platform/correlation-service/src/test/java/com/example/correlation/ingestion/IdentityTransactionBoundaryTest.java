package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class IdentityTransactionBoundaryTest {
    private static final String MAPPING = """
            name: split-ids
            topicPattern: 'split\\.events'
            fields:
              eventId: id
              executionId: orderNo
              aliasKey: linkedPaymentRef
            """;

    private static final String SOURCE = "split.events";
    private static final Instant TRANSPORT_TIME = Instant.parse("2026-01-01T00:00:00Z");

    private CorrelationService correlationService;
    private DeduplicationCache deduplication;
    private ExecutionAliasResolver aliasResolver;
    private InlineTransactionManager transactions;
    private EventIngestionService ingestion;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);
        deduplication = new DeduplicationCache();
        aliasResolver = new PassthroughAliasResolver();
        transactions = new InlineTransactionManager();

        SourceMapping mapping = parse(MAPPING);
        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mapping.matches(topic) ? mapping : SourceMapping.identity();
            }
        };

        ingestion = new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                deduplication,
                new EventTimeResolver(),
                registry,
                aliasResolver,
                transactions);
    }

    @Test
    void identityIsDecidedInsideTheUnitOfWorkThatStoresTheObservation() {
        ingest("""
                {"id":"e1","orderNo":"ORD-1","linkedPaymentRef":"PAY-9"}""");

        InOrder order = inOrder(correlationService);
        order.verify(correlationService).processEvent(any(IngestedEvent.class));

        assertThat(transactions.hasCommitted()).isTrue();
        assertThat(transactions.hasRolledBack()).isFalse();
    }

    @Test
    void aFailureStoringTheObservationRollsTheIdentityDecisionBack() {
        doThrow(new DataAccessResourceFailureException("database down"))
                .when(correlationService).processEvent(any(IngestedEvent.class));

        assertThatThrownBy(() -> ingest("""
                {"id":"e2","orderNo":"ORD-2","linkedPaymentRef":"PAY-8"}"""))
                .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(transactions.hasRolledBack()).isTrue();
        assertThat(transactions.hasCommitted()).isFalse();
    }

    @Test
    void anEventIsNotRememberedAsSeenUntilItsTransactionCommits() {
        doThrow(new DataAccessResourceFailureException("database down"))
                .when(correlationService).processEvent(any(IngestedEvent.class));

        String event = """
                {"id":"e3","orderNo":"ORD-3","linkedPaymentRef":"PAY-7"}""";
        assertThatThrownBy(() -> ingest(event))
                .isInstanceOf(DataAccessResourceFailureException.class);

        org.mockito.Mockito.reset(correlationService);
        IngestionResult retry = ingestion.ingest(observation(event));

        assertThat(retry.isAccepted()).isTrue();
        verify(correlationService).processEvent(any(IngestedEvent.class));
    }

    @Test
    void aRedeliveryAfterACommitIsRecognised() {
        String event = """
                {"id":"e4","orderNo":"ORD-4","linkedPaymentRef":"PAY-6"}""";

        assertThat(ingest(event).isAccepted()).isTrue();
        assertThat(ingestion.ingest(observation(event)).outcome())
                .isEqualTo(IngestionResult.Outcome.DUPLICATE);

        verify(correlationService, times(1)).processEvent(any(IngestedEvent.class));
    }

    @Test
    void anUnidentifiableObservationOpensNoTransaction() {
        IngestionResult result = ingestion.ingest(observation("""
                {"orderNo":"ORD-5"}"""));

        assertThat(result.outcome()).isEqualTo(IngestionResult.Outcome.NOT_CORRELATABLE);
        assertThat(transactions.hasCommitted()).isFalse();
        assertThat(transactions.hasRolledBack()).isFalse();
        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    private IngestionResult ingest(String body) {
        return ingestion.ingest(observation(body));
    }

    private static RawObservation observation(String body) {
        return new RawObservation(SOURCE, body, TRANSPORT_TIME, null, null, Map.of());
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
