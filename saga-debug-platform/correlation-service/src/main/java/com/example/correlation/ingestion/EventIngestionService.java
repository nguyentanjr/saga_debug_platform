package com.example.correlation.ingestion;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;

@Service
public class EventIngestionService {
    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);

    private final CorrelationService correlationService;
    private final ObjectMapper objectMapper;
    private final SourceCompletenessCheck completeness;
    private final DeduplicationCache deduplicationCache;
    private final EventTimeResolver eventTimeResolver;
    private final SourceMappingRegistry sourceMappings;
    private final ExecutionAliasResolver aliasResolver;

    private final TransactionTemplate transactions;

    public EventIngestionService(CorrelationService correlationService,
            ObjectMapper objectMapper,
            SourceCompletenessCheck completeness,
            DeduplicationCache deduplicationCache,
            EventTimeResolver eventTimeResolver,
            SourceMappingRegistry sourceMappings,
            ExecutionAliasResolver aliasResolver,
            PlatformTransactionManager transactionManager) {
        this.correlationService = correlationService;
        this.objectMapper = objectMapper;
        this.completeness = completeness;
        this.deduplicationCache = deduplicationCache;
        this.eventTimeResolver = eventTimeResolver;
        this.sourceMappings = sourceMappings;
        this.aliasResolver = aliasResolver;
        this.transactions = new TransactionTemplate(transactionManager);
    }

    public IngestionResult ingest(RawObservation raw) {
        SourceMapping mapping = sourceMappings.forTopic(raw.origin(), raw.sourceRef());

        String systemName = mapping.systemName() != null
                ? mapping.systemName()
                : ExecutionKey.DEFAULT_SOURCE;

        JsonNode payload;
        try {
            payload = objectMapper.reader()
                    .without(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION)
                    .readTree(raw.payload());
        } catch (Exception e) {
            log.error("Discarding unreadable observation from {}: {}",
                    raw.sourceRef(), e.getMessage());
            return IngestionResult.unreadable(e.getMessage());
        }

        completeness.report(completeness.gaps(payload, mapping, raw),
                mapping.getName(), raw.sourceRef());

        String eventId = mapping.eventId(payload, raw).orElse(raw.fallbackEventId());
        if (eventId == null) {
            String reason = "no value at '"
                    + SourceMapping.describe(mapping.getFields().getEventId())
                    + "' and the transport offered no identity";
            log.error("Cannot identify observation from {}: {}", raw.sourceRef(), reason);
            return IngestionResult.notCorrelatable(reason);
        }

        String dedupKey = systemName + ExecutionKey.SEPARATOR + eventId;

        if (deduplicationCache.seen(dedupKey)
                && !deduplicationCache.seenOnDifferentRecord(dedupKey, raw.fallbackEventId())) {
            log.debug("Skipping already-processed event {} from {}", eventId, raw.sourceRef());
            return IngestionResult.duplicate(eventId);
        }

        EventTimeResolver.Resolved resolved = eventTimeResolver.resolveValue(
                mapping.eventTime(payload, raw).orElse(null), raw.transportTime());
        if (!resolved.isObserved()) {
            log.warn("Event {} from {} has no usable timestamp at '{}'; using transport time {}",
                    eventId, raw.sourceRef(),
                    SourceMapping.describe(mapping.getFields().getEventTime()),
                    raw.transportTime());
        }

        String traceId = mapping.traceId(payload, raw).orElse(null);
        Correlation correlation = correlate(mapping, systemName, payload, raw, traceId);
        if (correlation == null) {
            String reason = "no value at '"
                    + SourceMapping.describe(mapping.getFields().getExecutionId())
                    + "', and no propagated trace context to fall back on";
            log.error("Cannot correlate observation from {}: {}. Declare the correlation key "
                    + "for this source under ingestion/*.yaml, or have the system propagate "
                    + "trace context.", raw.sourceRef(), reason);
            return IngestionResult.notCorrelatable(reason);
        }

        ExecutionKey alias = mapping.aliasKey(payload, raw)
                .map(key -> ExecutionKey.of(mapping.aliasNamespace(), key))
                .orElse(null);

        transactions.executeWithoutResult(status -> {
            ExecutionKey correlated = aliasResolver.canonical(
                    correlation.key(), alias, eventId, resolved.eventTime());

            CorrelationBasis basis = correlated.equals(correlation.key())
                    ? correlation.basis()
                    : CorrelationBasis.ALIAS_LINK;

            correlationService.processEvent(observationOf(
                    mapping, raw, payload, correlated.source(), eventId, correlated, basis,
                    resolved, traceId, raw.fallbackEventId()));
        });

        deduplicationCache.markProcessed(dedupKey, raw.fallbackEventId());
        return IngestionResult.accepted(eventId);
    }

    private IngestedEvent observationOf(SourceMapping mapping, RawObservation raw,
            JsonNode payload, String filedUnder, String eventId, ExecutionKey correlated,
            CorrelationBasis basis, EventTimeResolver.Resolved time, String traceId, String transportRef) {
        return new IngestedEvent(
                new IngestedEvent.Identity(filedUnder, correlated, eventId,
                        mapping.businessKey(payload, raw).orElse(null), basis),
                mapping.getWorkflow(),
                mapping.eventType(payload, raw).orElse(raw.sourceRef()),
                mapping.stepName(payload, raw).orElse("UNKNOWN"),
                mapping.serviceName(payload, raw).orElse("unknown"),
                time.eventTime(),
                mapping.redact(raw.payload()),
                Instant.now(),
                new IngestedEvent.Causality(
                        mapping.causationId(payload, raw).orElse(null),
                        mapping.spanId(payload, raw).orElse(null),
                        mapping.parentSpanId(payload, raw).orElse(null),
                        traceId),
                transportRef,
                time.source());
    }

    private record Correlation(ExecutionKey key, CorrelationBasis basis) {
    }

    private Correlation correlate(SourceMapping mapping, String sourceName,
            JsonNode payload, RawObservation raw, String traceId) {
        String declared = mapping.executionId(payload, raw).orElse(null);
        if (declared != null) {
            return new Correlation(ExecutionKey.of(sourceName, declared),
                    CorrelationBasis.DECLARED_KEY);
        }
        if (traceId != null && !traceId.isBlank()) {
            log.info("Observation from {} carries no declared correlation key; correlating on "
                    + "propagated trace context {}", raw.sourceRef(), traceId);
            return new Correlation(ExecutionKey.of(sourceName, traceId),
                    CorrelationBasis.PROPAGATED_CONTEXT);
        }
        return null;
    }
}
