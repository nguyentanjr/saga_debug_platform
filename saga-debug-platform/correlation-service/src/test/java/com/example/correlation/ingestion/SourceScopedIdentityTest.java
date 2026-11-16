package com.example.correlation.ingestion;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SourceScopedIdentityTest {
    private static final Instant TRANSPORT_TIME = Instant.parse("2026-01-01T12:00:00Z");

    private static final String SHOP = """
            name: shop
            topicPattern: 'shop\\.events'
            fields:
              eventId: id
              executionId: orderNo
              eventType: kind
            """;

    private static final String PARTNER = """
            name: partner
            topicPattern: 'partner\\.events'
            fields:
              eventId: id
              executionId: orderNo
              eventType: kind
            """;

    private CorrelationService correlationService;
    private DeduplicationCache deduplicationCache;
    private EventIngestionService ingestion;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);
        deduplicationCache = new DeduplicationCache();
        ingestion = new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                deduplicationCache,
                new EventTimeResolver(),
                registryOf(SHOP, PARTNER),
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager());
    }

    @Test
    void twoSourcesUsingTheSameExecutionIdAreTwoExecutions() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");
        ingest("partner.events", """
                {"id":"p1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested).hasSize(2);
        assertThat(ingested.get(0).sagaId()).isEqualTo("shop::ORD-A");
        assertThat(ingested.get(1).sagaId()).isEqualTo("partner::ORD-A");
        assertThat(ingested.get(0).sagaId()).isNotEqualTo(ingested.get(1).sagaId());
    }

    @Test
    void theRawIdentifierIsPreservedAlongsideTheNamespace() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        IngestedEvent ingested = allIngested().get(0);
        assertThat(ingested.executionKey().source()).isEqualTo("shop");
        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-A");
        assertThat(ingested.sourceName()).isEqualTo("shop");
    }

    @Test
    void twoSourcesUsingTheSameEventIdAreTwoObservations() {
        ingest("shop.events", """
                {"id":"1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");
        ingest("partner.events", """
                {"id":"1","orderNo":"ORD-B","kind":"ORDER_CREATED"}""");

        assertThat(allIngested()).hasSize(2);
    }

    @Test
    void anEventFromAnotherSourceNeverReachesAnAlreadyTerminalExecution() {
        ingest("shop.events", """
                {"id":"s9","orderNo":"ORD-DONE","kind":"ORDER_COMPLETED"}""");
        ingest("partner.events", """
                {"id":"p9","orderNo":"ORD-DONE","kind":"INVENTORY_FAILED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested.get(0).sagaId()).isEqualTo("shop::ORD-DONE");
        assertThat(ingested.get(1).sagaId()).isEqualTo("partner::ORD-DONE");
    }

    @Test
    void aReplayFromTheSameSourceIsStillADuplicate() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");
        IngestionResult second = ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        assertThat(second.outcome()).isEqualTo(IngestionResult.Outcome.DUPLICATE);
        assertThat(allIngested()).hasSize(1);
    }

    @Test
    void observationsArrivingOutOfOrderKeepTheirOwnIdentityAndTime() {
        ingest("shop.events", """
                {"id":"s2","orderNo":"ORD-A","kind":"PAYMENT_COMPLETED",
                 "timestamp":"2026-01-01T10:00:05Z"}""");
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED",
                 "timestamp":"2026-01-01T10:00:00Z"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested).hasSize(2);
        assertThat(ingested.get(0).sagaId()).isEqualTo(ingested.get(1).sagaId());
        assertThat(ingested.get(0).eventTime()).isAfter(ingested.get(1).eventTime());
    }

    @Test
    void aDeclaredCorrelationKeyIsRecordedAsSuch() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        assertThat(allIngested().get(0).correlationBasis())
                .isEqualTo(CorrelationBasis.DECLARED_KEY);
    }

    @Test
    void propagatedTraceContextCorrelatesWhenNoBusinessKeyExists() {
        IngestionResult result = ingest("shop.events", """
                {"id":"s1","kind":"ORDER_CREATED",
                 "traceId":"4bf92f3577b34da6a3ce929d0e0e4736"}""");

        assertThat(result.isAccepted()).isTrue();
        IngestedEvent ingested = allIngested().get(0);
        assertThat(ingested.executionKey().rawId())
                .isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(ingested.correlationBasis()).isEqualTo(CorrelationBasis.PROPAGATED_CONTEXT);
    }

    @Test
    void aDeclaredKeyIsPreferredOverTraceContext() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED",
                 "traceId":"4bf92f3577b34da6a3ce929d0e0e4736"}""");

        IngestedEvent ingested = allIngested().get(0);
        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-A");
        assertThat(ingested.correlationBasis()).isEqualTo(CorrelationBasis.DECLARED_KEY);
    }

    @Test
    void anObservationWithNothingOnTheLadderIsRefusedRatherThanGuessedAt() {
        IngestionResult result = ingest("shop.events", """
                {"id":"s1","kind":"ORDER_CREATED","customer":"acme"}""");

        assertThat(result.outcome()).isEqualTo(IngestionResult.Outcome.NOT_CORRELATABLE);
        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    @Test
    void theRefusalNamesBothRungsThatWereTried() {
        IngestionResult result = ingest("shop.events", """
                {"id":"s1","kind":"ORDER_CREATED"}""");

        assertThat(result.reason()).contains("orderNo").contains("trace context");
    }

    @Test
    void anUnmappedSourceGetsTheDefaultNamespace() {
        ingest("something.else", """
                {"eventId":"x1","sagaId":"ORD-A","eventType":"ORDER_CREATED"}""");

        assertThat(allIngested().get(0).sagaId()).isEqualTo("default::ORD-A");
    }

    private IngestionResult ingest(String sourceRef, String payload) {
        return ingestion.ingest(new RawObservation(
                sourceRef, payload, TRANSPORT_TIME, "fallback", null, Map.of()));
    }

    private List<IngestedEvent> allIngested() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService, org.mockito.Mockito.atLeast(0)).processEvent(captor.capture());
        return captor.getAllValues();
    }

    private static SourceMappingRegistry registryOf(String... mappingYaml) {
        List<SourceMapping> mappings = new ArrayList<>();
        for (String yaml : mappingYaml) {
            mappings.add(parse(yaml));
        }
        return new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mappings.stream()
                        .filter(mapping -> mapping.matches(topic))
                        .findFirst()
                        .orElseGet(SourceMapping::identity);
            }
        };
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
