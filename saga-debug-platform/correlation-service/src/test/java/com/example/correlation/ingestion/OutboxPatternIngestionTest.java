package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.messaging.SagaEventConsumer;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OutboxPatternIngestionTest {
    private static final String ROOM_TOPIC = "room-booking.outbox.events";
    private static final String PAYMENT_TOPIC = "payment.outbox.events";
    private static final List<String> ALL_TOPICS = List.of(ROOM_TOPIC, PAYMENT_TOPIC,
            "room-booking.inbox.events", "payment.inbox.events");

    private static final String SAGA_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6";

    private CorrelationService correlationService;
    private SagaEventConsumer consumer;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);

        SourceMappingRegistry registry = declaredMappings();

        consumer = new SagaEventConsumer(new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new com.example.correlation.ingestion.SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry,
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager()));
    }

    @Test
    void bothOutboxTopicsHaveTheirOwnMapping() {
        SourceMappingRegistry registry = declaredMappings();

        assertThat(registry.forTopic(ROOM_TOPIC).getName()).isEqualTo("hotel-saga-room-booking");
        assertThat(registry.forTopic(PAYMENT_TOPIC).getName()).isEqualTo("hotel-saga-payment");
    }

    @Test
    void theCorrelationKeyIsReadFromTheRecordKey() {
        consumer.handleEvent(roomRecord("""
                {"status":"BOOKED"}""", "evt-room-1"));

        assertThat(captured().executionKey().rawId()).isEqualTo(SAGA_ID);
    }

    @Test
    void theIdentityIsReadFromAHeader() {
        consumer.handleEvent(roomRecord("""
                {"status":"BOOKED"}""", "evt-room-1"));

        assertThat(captured().eventId()).isEqualTo("evt-room-1");
    }

    @Test
    void theOutcomeIsReadFromTheBodyAndTranslated() {
        consumer.handleEvent(roomRecord("""
                {"status":"BOOKED"}""", "evt-room-1"));

        assertThat(captured().eventType()).isEqualTo("ROOM_BOOKED");
    }

    @Test
    void aRejectionIsDistinguishedFromABookingEvenThoughTheHeaderTypeIsIdentical() {
        consumer.handleEvent(roomRecord("""
                {"status":"REJECTED"}""", "evt-room-2"));

        assertThat(captured().eventType()).isEqualTo("ROOM_REJECTED");
    }

    @Test
    void theSameStatusValueOnTwoTopicsTranslatesToTwoDifferentEvents() {
        consumer.handleEvent(roomRecord("""
                {"status":"CANCELLED"}""", "evt-room-3"));
        assertThat(captured().eventType()).isEqualTo("ROOM_CANCELLED");

        setUp();
        consumer.handleEvent(paymentRecord("""
                {"status":"CANCELLED"}""", "evt-pay-3"));
        assertThat(captured().eventType()).isEqualTo("PAYMENT_REVERSED");
    }

    @Test
    void thePaymentTopicIsReadWithItsOwnVocabulary() {
        consumer.handleEvent(paymentRecord("""
                {"status":"FAILED"}""", "evt-pay-1"));

        IngestedEvent ingested = captured();
        assertThat(ingested.executionKey().rawId()).isEqualTo(SAGA_ID);
        assertThat(ingested.eventType()).isEqualTo("PAYMENT_REJECTED");
    }

    @Test
    void successIsSignalledByRequestedBecauseCompletedIsUnreachable() {
        consumer.handleEvent(paymentRecord("""
                {"status":"REQUESTED"}""", "evt-pay-2"));

        assertThat(captured().eventType()).isEqualTo("PAYMENT_ACCEPTED");
    }

    @Test
    void anObservationWithNoKeyIsNotIngested() {
        ConsumerRecord<String, String> keyless = new ConsumerRecord<>(
                ROOM_TOPIC, 0, 0L, 0L, TimestampType.CREATE_TIME, 0, 0,
                null, """
                {"status":"BOOKED"}""", headers("evt-x"), Optional.empty());

        consumer.handleEvent(keyless);

        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    @Test
    void theWorkflowSpecificationRecognisesEveryMappedEventType() {
        WorkflowSpec spec = roomReservation();

        Set<String> mapped = new TreeSet<>();
        SourceMappingRegistry mappings = declaredMappings();
        for (String topic : ALL_TOPICS) {
            mapped.addAll(mappings.forTopic(topic).getEventTypes().values());
        }

        assertThat(mapped).contains("BOOK_ROOM_REQUESTED", "ROOM_BOOKED");
        assertThat(mapped).filteredOn(type -> !MAPPED_BUT_UNREACHABLE.contains(type))
                .allSatisfy(type -> assertThat(spec.classifyEvent(type)).as(type).isPresent());
    }

    private static final Set<String> MAPPED_BUT_UNREACHABLE =
            Set.of("REFUND_PAYMENT_REQUESTED", "PAYMENT_REVERSED");

    @Test
    void theMappedButUnreachableNamesAreNotPartOfTheWorkflow() {
        WorkflowSpec spec = roomReservation();

        assertThat(MAPPED_BUT_UNREACHABLE)
                .allSatisfy(type -> assertThat(spec.classifyEvent(type)).as(type).isEmpty());
    }

    @Test
    void aRejectedPaymentRoutesToCompensationNotToOutrightFailure() {
        WorkflowSpec spec = roomReservation();

        assertThat(spec.classifyEvent("PAYMENT_REJECTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPENSATING);
        assertThat(spec.classifyEvent("ROOM_REJECTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.FAILED);
    }

    @Test
    void aPaymentAcceptedCompletesTheSaga() {
        WorkflowSpec spec = roomReservation();

        assertThat(spec.classifyEvent("PAYMENT_ACCEPTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPLETED);
    }

    @Test
    void releasingTheRoomResolvesTheSagaAsCompensated() {
        WorkflowSpec spec = roomReservation();

        assertThat(spec.classifyEvent("ROOM_CANCELLED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPENSATED);
    }

    private static String declarationsDirectory() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        return root.resolve("declarations").toString();
    }

    static SourceMappingRegistry declaredMappings() {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", declarationsDirectory());
        registry.load();
        return registry;
    }

    @org.junit.jupiter.api.Test
    void theHotelWorkflowDeclaresTheDeploymentItsServicesRunIn() {
        org.assertj.core.api.Assertions.assertThat(roomReservation().getRuntimeScope()).isEqualTo("hotel-real");
    }

    private static WorkflowSpec roomReservation() {
        WorkflowRegistry registry = new WorkflowRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", declarationsDirectory());
        registry.load();
        return registry.byType("ROOM_RESERVATION").orElseThrow();
    }

    private ConsumerRecord<String, String> roomRecord(String body, String eventId) {
        return record(ROOM_TOPIC, body, eventId, "RoomUpdated");
    }

    private ConsumerRecord<String, String> paymentRecord(String body, String eventId) {
        return record(PAYMENT_TOPIC, body, eventId, "PaymentUpdated");
    }

    private ConsumerRecord<String, String> record(String topic, String body,
            String eventId, String headerType) {
        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader("id", eventId.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("eventType", headerType.getBytes(StandardCharsets.UTF_8)));

        return new ConsumerRecord<>(topic, 0, 0L, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, 0, 0, SAGA_ID, body, headers, Optional.empty());
    }

    private static org.apache.kafka.common.header.Headers headers(String eventId) {
        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader("id", eventId.getBytes(StandardCharsets.UTF_8)));
        return headers;
    }

    private IngestedEvent captured() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }
}
