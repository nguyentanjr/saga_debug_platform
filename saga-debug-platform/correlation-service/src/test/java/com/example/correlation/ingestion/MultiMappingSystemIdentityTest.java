package com.example.correlation.ingestion;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionAlias;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * One observed system read by several mappings.
 *
 * <h2>What went wrong before</h2>
 * The namespace half of an {@link com.example.correlation.domain.ExecutionKey} was taken
 * from the mapping's own {@code name}, which quietly asserted that one mapping observes one
 * system. That holds only for systems simple enough to read with a single file.
 *
 * <p>{@code semotpan/saga-orchestration} is not one of them. Its saga publishes on four
 * topics whose payloads all carry a field called {@code status}, and {@code CANCELLED}
 * means "the room booking was released" on one topic and "the payment was reversed" on
 * another — so one mapping cannot translate it, and four mappings became four id spaces.
 * A single saga id was filed as up to four half-finished executions, each looking stuck,
 * with an ORPHAN_EVENT raised against the fragment whose first event starts no workflow.
 *
 * <p>The documented remedy was {@code aliasSource}, a declared cross-source link. It
 * resolved correctly and then the observation was <em>rejected and dropped</em>:
 * {@code EventIngestionService} passed the reading mapping's name as the identity's source
 * while the alias had moved the key into another namespace, and {@code IngestedEvent
 * .Identity} refuses an identity whose two halves disagree. It was right to refuse — the
 * database enforces the same invariant with a CHECK tying {@code source_name} to
 * {@code saga_id} — so the fix belongs upstream, at the mapping, not at the resolver.
 *
 * <p>These tests pin both halves: several mappings can now declare one {@code system}, and
 * a declared cross-system alias no longer contradicts itself. They also pin what must stay
 * impossible, which is everything else — see the second half of this class.
 */
class MultiMappingSystemIdentityTest {

    private static final Instant TRANSPORT_TIME = Instant.parse("2026-01-01T12:00:00Z");

    // ── one system, four mappings: the shape that motivated this ──────────────

    private static final String ROOM_REPLIES = """
            name: hotel-saga-room-booking
            system: hotel-saga
            topicPattern: 'room-booking\\.outbox\\.events'
            fields:
              eventId: id
              executionId: sagaRef
              eventType: status
            eventTypes:
              "BOOKED": ROOM_BOOKED
              "CANCELLED": ROOM_CANCELLED
            """;

    private static final String PAYMENT_REPLIES = """
            name: hotel-saga-payment
            system: hotel-saga
            topicPattern: 'payment\\.outbox\\.events'
            fields:
              eventId: id
              executionId: sagaRef
              eventType: status
            eventTypes:
              "REQUESTED": PAYMENT_ACCEPTED
              "CANCELLED": PAYMENT_REVERSED
            """;

    private static final String ROOM_COMMANDS = """
            name: hotel-saga-room-booking-commands
            system: hotel-saga
            topicPattern: 'room-booking\\.inbox\\.events'
            fields:
              eventId: id
              executionId: sagaRef
              eventType: type
            eventTypes:
              "REQUEST": BOOK_ROOM_REQUESTED
            """;

    private static final String PAYMENT_COMMANDS = """
            name: hotel-saga-payment-commands
            system: hotel-saga
            topicPattern: 'payment\\.inbox\\.events'
            fields:
              eventId: id
              executionId: sagaRef
              eventType: type
            eventTypes:
              "REQUEST": TAKE_PAYMENT_REQUESTED
            """;

    /** A different company's system that happens to number its sagas the same way. */
    private static final String OTHER_SYSTEM = """
            name: partner-booking
            system: partner
            topicPattern: 'partner\\.events'
            fields:
              eventId: id
              executionId: sagaRef
              eventType: status
            """;

    /** No {@code system:} at all — the shape every existing mapping still has. */
    private static final String UNDECLARED_SYSTEM = """
            name: legacy-shop
            topicPattern: 'legacy\\.events'
            fields:
              eventId: id
              executionId: sagaRef
              eventType: status
            """;

    /**
     * A gateway that publishes its own reference and the hotel saga's in the same event,
     * and says in its mapping which system the second one names.
     */
    private static final String GATEWAY_WITH_DECLARED_ALIAS = """
            name: booking-gateway
            system: gateway
            topicPattern: 'gateway\\.events'
            aliasSource: hotel-saga
            fields:
              eventId: id
              executionId: bookingRef
              aliasKey: sagaRef
              eventType: status
            """;

    /** The same gateway, with the cross-system declaration removed. */
    private static final String GATEWAY_WITHOUT_DECLARATION = """
            name: booking-gateway
            system: gateway
            topicPattern: 'gateway\\.events'
            fields:
              eventId: id
              executionId: bookingRef
              aliasKey: sagaRef
              eventType: status
            """;

    private CorrelationService correlationService;
    private Map<ExecutionAlias.Key, ExecutionAlias> aliasRows;
    private Map<String, SagaInstance> instances;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);
        aliasRows = new LinkedHashMap<>();
        instances = new LinkedHashMap<>();
    }

    // ══ what must now work ════════════════════════════════════════════════════

    /**
     * The headline. Two mappings, two names, one declared system: one id space.
     *
     * <p>Before, these were {@code hotel-saga-room-booking::SAGA-1} and
     * {@code hotel-saga-payment::SAGA-1} — two executions, each with one event, each
     * looking half-finished.
     */
    @Test
    void severalMappingsDeclaringOneSystemShareOneNamespace() {
        EventIngestionService ingestion = ingestionOf(ROOM_REPLIES, PAYMENT_REPLIES);

        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-1","status":"BOOKED"}""");
        ingest(ingestion, "payment.outbox.events", """
                {"id":"p1","sagaRef":"SAGA-1","status":"REQUESTED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested).hasSize(2);
        assertThat(ingested).allSatisfy(event ->
                assertThat(event.sagaId()).isEqualTo("hotel-saga::SAGA-1"));

        // Both halves of the identity agree, which is what the database's CHECK requires.
        assertThat(ingested).allSatisfy(event ->
                assertThat(event.sourceName()).isEqualTo(event.executionKey().source()));
    }

    /** The real topology: four topics, four mappings, one saga id, one execution. */
    @Test
    void theSameSagaIdAcrossFourTopicsIsOneExecution() {
        EventIngestionService ingestion =
                ingestionOf(ROOM_COMMANDS, ROOM_REPLIES, PAYMENT_COMMANDS, PAYMENT_REPLIES);

        ingest(ingestion, "room-booking.inbox.events", """
                {"id":"c1","sagaRef":"SAGA-7","type":"REQUEST"}""");
        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-7","status":"BOOKED"}""");
        ingest(ingestion, "payment.inbox.events", """
                {"id":"c2","sagaRef":"SAGA-7","type":"REQUEST"}""");
        ingest(ingestion, "payment.outbox.events", """
                {"id":"p1","sagaRef":"SAGA-7","status":"REQUESTED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested).hasSize(4);
        assertThat(ingested).extracting(IngestedEvent::sagaId)
                .containsOnly("hotel-saga::SAGA-7");

        // Each mapping still reads its own topic's vocabulary; sharing a namespace does
        // not mean sharing a translation.
        assertThat(ingested).extracting(IngestedEvent::eventType)
                .containsExactly("BOOK_ROOM_REQUESTED", "ROOM_BOOKED",
                        "TAKE_PAYMENT_REQUESTED", "PAYMENT_ACCEPTED");
    }

    /**
     * A mapping that declares no system keeps its own name as the namespace, so every
     * mapping written before {@code system:} existed behaves exactly as it did.
     */
    @Test
    void aMappingWithoutADeclaredSystemKeepsItsOwnNameAsTheNamespace() {
        EventIngestionService ingestion = ingestionOf(UNDECLARED_SYSTEM);

        ingest(ingestion, "legacy.events", """
                {"id":"l1","sagaRef":"SAGA-1","status":"OPENED"}""");

        assertThat(allIngested().get(0).sagaId()).isEqualTo("legacy-shop::SAGA-1");
    }

    /**
     * A declared cross-system alias now survives ingestion instead of being dropped.
     *
     * <p>This is the exact regression. The gateway's event carries both its own booking
     * reference and the hotel saga's id, and its mapping says the second one names a run in
     * {@code hotel-saga}. The resolver folds the two; the observation must then be filed
     * under the namespace it was folded into, and must reach the correlation service at all.
     */
    @Test
    void aDeclaredCrossSystemAliasIsIngestedRatherThanDropped() {
        EventIngestionService ingestion =
                ingestionOf(ROOM_REPLIES, GATEWAY_WITH_DECLARED_ALIAS);

        // The hotel saga is already running under its own id, so the gateway's reference is
        // the newcomer and the fold has to move it across namespaces -- which is exactly
        // the case that used to be rejected.
        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-1","status":"BOOKED"}""");
        instances.put("hotel-saga::SAGA-1", instanceOf("hotel-saga::SAGA-1", TRANSPORT_TIME));

        IngestionResult result = ingest(ingestion, "gateway.events", """
                {"id":"g1","bookingRef":"BK-1","sagaRef":"SAGA-1","status":"CONFIRMED"}""");

        // Previously: IllegalArgumentException "identity disagrees with itself", the record
        // exhausted its retries and the event was never stored.
        assertThat(result.isAccepted()).isTrue();

        IngestedEvent gatewayEvent = allIngested().get(1);
        assertThat(gatewayEvent.sagaId()).isEqualTo("hotel-saga::SAGA-1");
        assertThat(gatewayEvent.correlationBasis()).isEqualTo(CorrelationBasis.ALIAS_LINK);
        // The identity is internally consistent, so the CHECK on saga_events holds.
        assertThat(gatewayEvent.sourceName()).isEqualTo("hotel-saga");
    }

    /**
     * The link may arrive after both halves already have executions of their own. The fold
     * then has to move what was filed under the loser, and later observations from either
     * side must resolve to the survivor.
     */
    @Test
    void aLateAliasFoldsTwoExecutionsThatWereAlreadyRunning() {
        EventIngestionService ingestion =
                ingestionOf(ROOM_REPLIES, GATEWAY_WITH_DECLARED_ALIAS);

        // Both run independently first: neither event carries the other's identifier.
        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-2","status":"BOOKED"}""");
        instances.put("hotel-saga::SAGA-2", instanceOf("hotel-saga::SAGA-2", TRANSPORT_TIME));
        ingest(ingestion, "gateway.events", """
                {"id":"g1","bookingRef":"BK-2","status":"OPENED"}""");
        instances.put("gateway::BK-2",
                instanceOf("gateway::BK-2", TRANSPORT_TIME.plusSeconds(1)));

        assertThat(allIngested()).extracting(IngestedEvent::sagaId)
                .containsExactly("hotel-saga::SAGA-2", "gateway::BK-2");

        // Only now does an event mention both.
        ingest(ingestion, "gateway.events", """
                {"id":"g2","bookingRef":"BK-2","sagaRef":"SAGA-2","status":"CONFIRMED"}""");

        // The survivor is the execution observed first, and the linking event is filed there.
        assertThat(allIngested().get(2).sagaId()).isEqualTo("hotel-saga::SAGA-2");

        // A later event from the gateway, carrying only its own reference, follows.
        ingest(ingestion, "gateway.events", """
                {"id":"g3","bookingRef":"BK-2","status":"SETTLED"}""");
        assertThat(allIngested().get(3).sagaId()).isEqualTo("hotel-saga::SAGA-2");
    }

    // ══ what must stay impossible ═════════════════════════════════════════════

    /**
     * Two genuinely different systems that happen to use the same identifier stay apart.
     * This is the protection {@code ExecutionKey} exists for, and declaring {@code system:}
     * must not have weakened it: the systems differ, so the namespaces differ, so the runs
     * never meet.
     */
    @Test
    void twoDifferentSystemsSharingARawIdentifierAreNotMerged() {
        EventIngestionService ingestion = ingestionOf(ROOM_REPLIES, OTHER_SYSTEM);

        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-1","status":"BOOKED"}""");
        ingest(ingestion, "partner.events", """
                {"id":"x1","sagaRef":"SAGA-1","status":"BOOKED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested.get(0).sagaId()).isEqualTo("hotel-saga::SAGA-1");
        assertThat(ingested.get(1).sagaId()).isEqualTo("partner::SAGA-1");
        // Each key claims only itself (EXT-ALIAS-RACE); none names the other system's run.
        assertThat(aliasRows.values())
                .allSatisfy(row -> assertThat(row.getExecutionId())
                        .isEqualTo(ExecutionKey.of(row.getSourceName(), row.getAliasKey()).canonical()));
    }

    /**
     * An event carrying two identifiers is not enough on its own. Without
     * {@code aliasSource}, the alias is scoped to the gateway's own system and names
     * nothing in the hotel saga's — so the two runs stay separate even though the strings
     * match exactly.
     */
    @Test
    void anUndeclaredAliasDoesNotReachAnotherSystem() {
        EventIngestionService ingestion =
                ingestionOf(ROOM_REPLIES, GATEWAY_WITHOUT_DECLARATION);

        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-1","status":"BOOKED"}""");
        ingest(ingestion, "gateway.events", """
                {"id":"g1","bookingRef":"BK-1","sagaRef":"SAGA-1","status":"CONFIRMED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested.get(0).sagaId()).isEqualTo("hotel-saga::SAGA-1");
        assertThat(ingested.get(1).sagaId()).isEqualTo("gateway::BK-1");

        // The alias it did register is inside its own system, where it is harmless and true.
        assertThat(aliasRows.keySet())
                .allSatisfy(key -> assertThat(key.toString()).doesNotContain("hotel-saga"));
    }

    // ══ deduplication follows the system, not the file ════════════════════════

    /**
     * An event id is unique within the system that issued it, so a redelivery is a
     * duplicate whichever mapping reads it. The four mappings of one saga read four topics
     * of one outbox, and its row ids do not restart per topic.
     */
    @Test
    void aRedeliveryIsADuplicateAcrossTheMappingsOfOneSystem() {
        EventIngestionService ingestion = ingestionOf(ROOM_REPLIES, PAYMENT_REPLIES);

        assertThat(ingest(ingestion, "room-booking.outbox.events", """
                {"id":"e1","sagaRef":"SAGA-1","status":"BOOKED"}""").isAccepted()).isTrue();

        IngestionResult replay = ingest(ingestion, "payment.outbox.events", """
                {"id":"e1","sagaRef":"SAGA-1","status":"REQUESTED"}""");

        assertThat(replay.outcome()).isEqualTo(IngestionResult.Outcome.DUPLICATE);
        assertThat(allIngested()).hasSize(1);
    }

    /** The same replay within one mapping is a duplicate too, as it always was. */
    @Test
    void aReplayOfTheSameObservationIsStillOneObservation() {
        EventIngestionService ingestion = ingestionOf(ROOM_REPLIES);

        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-1","status":"BOOKED"}""");
        IngestionResult replay = ingest(ingestion, "room-booking.outbox.events", """
                {"id":"r1","sagaRef":"SAGA-1","status":"BOOKED"}""");

        assertThat(replay.outcome()).isEqualTo(IngestionResult.Outcome.DUPLICATE);
        assertThat(allIngested()).hasSize(1);
    }

    /**
     * Two systems numbering their events alike are still two observations. Namespacing the
     * deduplication key by the system rather than by the mapping preserves this: what
     * changed is which name is used, not that a name is used.
     */
    @Test
    void twoSystemsUsingTheSameEventIdAreTwoObservations() {
        EventIngestionService ingestion = ingestionOf(ROOM_REPLIES, OTHER_SYSTEM);

        ingest(ingestion, "room-booking.outbox.events", """
                {"id":"1","sagaRef":"SAGA-1","status":"BOOKED"}""");
        ingest(ingestion, "partner.events", """
                {"id":"1","sagaRef":"SAGA-2","status":"BOOKED"}""");

        assertThat(allIngested()).hasSize(2);
    }

    // ══ the mapping model itself ══════════════════════════════════════════════

    @Test
    void aMappingReportsTheSystemItObservesAndTheNameItIsKnownBy() {
        SourceMapping declared = parse(ROOM_REPLIES);
        assertThat(declared.getName()).isEqualTo("hotel-saga-room-booking");
        assertThat(declared.systemName()).isEqualTo("hotel-saga");

        SourceMapping undeclared = parse(UNDECLARED_SYSTEM);
        assertThat(undeclared.systemName()).isEqualTo(undeclared.getName());
    }

    /** An alias namespace defaults to the observed system, not to the file that read it. */
    @Test
    void anAliasWithoutADeclaredSourceStaysInsideItsOwnSystem() {
        assertThat(parse(GATEWAY_WITHOUT_DECLARATION).aliasNamespace()).isEqualTo("gateway");
        assertThat(parse(GATEWAY_WITH_DECLARED_ALIAS).aliasNamespace()).isEqualTo("hotel-saga");
    }

    /**
     * The real declaration files in {@code declarations/}, loaded the way the running
     * service loads them, rather than the inline mappings above. Two separate files, each
     * with its own name, both naming one system.
     */
    @Test
    void theShippedHotelSagaMappingsObserveOneSystem() {
        SourceMappingRegistry registry = OutboxPatternIngestionTest.declaredMappings();

        SourceMapping rooms = registry.forTopic("room-booking.outbox.events");
        SourceMapping payments = registry.forTopic("payment.outbox.events");

        assertThat(rooms.getName()).isNotEqualTo(payments.getName());
        assertThat(rooms.systemName()).isEqualTo("hotel-saga");
        assertThat(payments.systemName()).isEqualTo("hotel-saga");
        assertThat(registry.observedSystems()).contains("hotel-saga");
    }

    /** And read end to end, they put one saga id in one place. */
    @Test
    void theShippedHotelSagaMappingsPutOneSagaIdInOneExecution() {
        SourceMappingRegistry registry = OutboxPatternIngestionTest.declaredMappings();

        EventIngestionService ingestion = new EventIngestionService(
                correlationService, new ObjectMapper(), new SourceCompletenessCheck(),
                new DeduplicationCache(), new EventTimeResolver(), registry,
                aliasResolver(), new InlineTransactionManager());

        ingestion.ingest(new RawObservation("room-booking.outbox.events",
                "{\"status\":\"BOOKED\"}", TRANSPORT_TIME, null, "SAGA-9",
                Map.of("id", "r1")));
        ingestion.ingest(new RawObservation("payment.outbox.events",
                "{\"status\":\"REQUESTED\"}", TRANSPORT_TIME, null, "SAGA-9",
                Map.of("id", "p1")));

        assertThat(allIngested()).hasSize(2);
        assertThat(allIngested()).extracting(IngestedEvent::sagaId)
                .containsOnly("hotel-saga::SAGA-9");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private IngestionResult ingest(EventIngestionService ingestion,
            String sourceRef, String payload) {
        return ingestion.ingest(new RawObservation(
                sourceRef, payload, TRANSPORT_TIME, "fallback", null, Map.of()));
    }

    private List<IngestedEvent> allIngested() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService, atLeast(0)).processEvent(captor.capture());
        return captor.getAllValues();
    }

    private static SagaInstance instanceOf(String sagaId, Instant startedAt) {
        SagaInstance instance = new SagaInstance();
        instance.setSagaId(sagaId);
        instance.setStartTime(startedAt);
        return instance;
    }

    /**
     * A real {@link ExecutionAliasResolver} over in-memory repositories.
     *
     * <p>Mocking the resolver would test nothing here: the bug was the disagreement between
     * what it returns and what ingestion then claims about the result, so the fold has to
     * actually happen.
     */
    private EventIngestionService ingestionOf(String... mappingYaml) {
        List<SourceMapping> mappings = new ArrayList<>();
        for (String yaml : mappingYaml) {
            mappings.add(parse(yaml));
        }
        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mappings.stream()
                        .filter(mapping -> mapping.matches(topic))
                        .findFirst()
                        .orElseGet(SourceMapping::identity);
            }
        };

        return new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry,
                aliasResolver(),
                new InlineTransactionManager());
    }

    private ExecutionAliasResolver aliasResolver() {
        ExecutionAliasRepository aliases = mock(ExecutionAliasRepository.class);
        when(aliases.findById(any(ExecutionAlias.Key.class)))
                .thenAnswer(call -> Optional.ofNullable(aliasRows.get(call.getArgument(0))));
        when(aliases.save(any(ExecutionAlias.class))).thenAnswer(call -> {
            ExecutionAlias alias = call.getArgument(0);
            aliasRows.put(new ExecutionAlias.Key(alias.getSourceName(), alias.getAliasKey()),
                    alias);
            return alias;
        });
        when(aliases.findByExecutionId(anyString())).thenAnswer(call -> {
            String executionId = call.getArgument(0);
            List<ExecutionAlias> found = new ArrayList<>();
            aliasRows.values().forEach(alias -> {
                if (alias.getExecutionId().equals(executionId)) found.add(alias);
            });
            return found;
        });

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findById(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstances.findByIdForUpdate(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstances.save(any(SagaInstance.class))).thenAnswer(call -> {
            SagaInstance instance = call.getArgument(0);
            instances.put(instance.getSagaId(), instance);
            return instance;
        });
        org.mockito.Mockito.doAnswer(call -> {
            instances.remove(((SagaInstance) call.getArgument(0)).getSagaId());
            return null;
        }).when(sagaInstances).delete(any(SagaInstance.class));

        return new ExecutionAliasResolver(
                aliases,
                mock(SagaEventRepository.class),
                sagaInstances,
                mock(AnomalyRepository.class),
                mock(RcaResultRepository.class));
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
