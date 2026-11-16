package com.example.correlation.controller;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.ingestion.EventIngestionService;
import com.example.correlation.ingestion.SourceMappingInferrer;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class IngestControllerTest {
    private static final String SOURCE = "legacy.orders.events";
    private static final String TOKEN = "test-ingest-token";

    private final ObjectMapper mapper = new ObjectMapper();
    private CorrelationService correlationService;
    private IngestController controller;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);

        SourceMappingRegistry mappings = new SourceMappingRegistry();
        mappings.load();

        controller = new IngestController(new EventIngestionService(
                correlationService,
                mapper,
                new com.example.correlation.ingestion.SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                mappings,
                new com.example.correlation.ingestion.PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager()),
                new SourceMappingInferrer(), new IngestAuthorization(TOKEN));
    }

    @Test
    void aPostedEventTakesTheSamePathAsAConsumedOne() {
        ResponseEntity<Map<String, Object>> response = post("""
                {
                  "id": "http-1",
                  "type": "order.payment.captured",
                  "occurredAt": "2026-01-01T00:00:04Z",
                  "emittedBy": { "service": "billing-api" },
                  "context":   { "orderNumber": "ORD-77" }
                }
                """);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).containsEntry("accepted", 1);

        IngestedEvent ingested = captured();
        assertThat(ingested.eventId()).isEqualTo("http-1");
        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-77");
        assertThat(ingested.eventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(ingested.serviceName()).isEqualTo("billing-api");
    }

    @Test
    void aBatchIsAcceptedAndReportedPerEvent() {
        ResponseEntity<Map<String, Object>> response = post("""
                [
                  {"id":"b1","type":"order.created","occurredAt":"2026-01-01T00:00:00Z",
                   "context":{"orderNumber":"ORD-1"}},
                  {"id":"b2","type":"order.payment.captured","occurredAt":"2026-01-01T00:00:02Z",
                   "context":{"orderNumber":"ORD-1"}}
                ]
                """);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).containsEntry("received", 2).containsEntry("accepted", 2);
        verify(correlationService, times(2)).processEvent(any(IngestedEvent.class));
    }

    @Test
    void oneBadEventDoesNotCostTheCallerTheRest() {
        ResponseEntity<Map<String, Object>> response = post("""
                [
                  {"id":"g1","type":"order.created","occurredAt":"2026-01-01T00:00:00Z",
                   "context":{"orderNumber":"ORD-2"}},
                  {"id":"b1","type":"order.created","occurredAt":"2026-01-01T00:00:01Z"}
                ]
                """);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).containsEntry("accepted", 1).containsEntry("rejected", 1);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results =
                (List<Map<String, Object>>) response.getBody().get("results");
        assertThat(results.get(0)).containsEntry("outcome", "ACCEPTED");

        assertThat(results.get(1)).containsEntry("outcome", "NOT_CORRELATABLE");
        assertThat(results.get(1).get("reason").toString()).contains("context.orderNumber");
    }

    @Test
    void repostingAnEventIsRecognisedRatherThanStoredTwice() {
        String event = """
                {"id":"dup-1","type":"order.created","occurredAt":"2026-01-01T00:00:00Z",
                 "context":{"orderNumber":"ORD-3"}}
                """;

        assertThat(post(event).getBody()).containsEntry("accepted", 1);
        assertThat(post(event).getBody()).containsEntry("duplicates", 1);

        verify(correlationService, times(1)).processEvent(any(IngestedEvent.class));
    }

    @Test
    void anEventWithoutAnIdIsRejectedRatherThanGivenASyntheticOne() {
        ResponseEntity<Map<String, Object>> response = post("""
                {"type":"order.created","occurredAt":"2026-01-01T00:00:00Z",
                 "context":{"orderNumber":"ORD-4"}}
                """);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).containsEntry("rejected", 1);
    }

    @Test
    void anEmptyBatchIsRefused() {
        assertThat(post("[]").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void aTransientFailurePropagatesSoTheCallerRetries() {
        doThrow(new DataAccessResourceFailureException("database down"))
                .when(correlationService).processEvent(any(IngestedEvent.class));

        assertThatThrownBy(() -> post("""
                {"id":"t1","type":"order.created","occurredAt":"2026-01-01T00:00:00Z",
                 "context":{"orderNumber":"ORD-5"}}
                """))
                .isInstanceOf(DataAccessResourceFailureException.class);
    }

    private static final String EVENT = """
            {"id":"http-auth","type":"order.payment.captured","occurredAt":"2026-01-01T00:00:04Z",
             "emittedBy":{"service":"billing-api"},"context":{"orderNumber":"ORD-77"}}
            """;

    @Test
    void anEventWithoutTheTokenIsNotStored() throws Exception {
        JsonNode body = mapper.readTree(EVENT);

        assertThat(controller.ingest(SOURCE, body, Map.of()).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(controller.ingest(SOURCE, body, Map.of("authorization", "Bearer wrong")).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(controller.infer(SOURCE, body, null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verifyNoInteractions(correlationService);

        assertThat(controller.ingest(SOURCE, body, Map.of("authorization", "bearer " + TOKEN)).getStatusCode())
                .isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    void withNoTokenConfiguredTheEndpointIsClosedNotOpen() throws Exception {
        IngestController unconfigured = new IngestController(null, new SourceMappingInferrer(), new IngestAuthorization(""));

        assertThat(unconfigured.ingest(SOURCE, mapper.readTree(EVENT), Map.of("Authorization", "Bearer "))
                .getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void handInjectionIsGuardedByTheSameToken() {
        CorrelationService correlation = mock(CorrelationService.class);
        AdminEventController admin = new AdminEventController(correlation, null, null, new IngestAuthorization(TOKEN));
        Map<String, String> body = Map.of("sagaId", "S-1", "eventType", "ORDER_CREATED");

        assertThat(admin.injectEvent(body, null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verifyNoInteractions(correlation);
        assertThat(admin.injectEvent(body, "Bearer " + TOKEN).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void theCredentialIsNotOfferedToTheMappingAsATransportHeader() {
        assertThat(IngestController.normalise(Map.of("Authorization", "Bearer x", "X-Observation-Key", "S-1")))
                .containsOnlyKeys("x-observation-key");
    }

    private ResponseEntity<Map<String, Object>> post(String json) {
        try {
            JsonNode body = mapper.readTree(json);

            return controller.ingest(SOURCE, body, Map.of("Authorization", "Bearer " + TOKEN));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException("test payload is not JSON", e);
        }
    }

    private IngestedEvent captured() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }
}
