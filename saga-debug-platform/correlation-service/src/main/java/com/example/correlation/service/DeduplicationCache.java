package com.example.correlation.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class DeduplicationCache {
    private static final Logger log = LoggerFactory.getLogger(DeduplicationCache.class);

    private static final String NO_TRANSPORT_REF = "";

    private final Cache<String, String> cache;

    public DeduplicationCache() {
        this.cache = Caffeine.newBuilder()
                .maximumSize(100_000)
                .expireAfterWrite(Duration.ofHours(1))
                .build();
        log.info("DeduplicationCache initialized: maxSize=100000, TTL=1h");
    }

    public boolean seen(String eventId) {
        return eventId != null && cache.getIfPresent(eventId) != null;
    }

    public boolean seenOnDifferentRecord(String eventId, String transportRef) {
        if (eventId == null || transportRef == null || transportRef.isBlank()) {
            return false;
        }
        String recorded = cache.getIfPresent(eventId);
        return recorded != null && !NO_TRANSPORT_REF.equals(recorded) && !recorded.equals(transportRef);
    }

    public void markProcessed(String eventId) {
        markProcessed(eventId, null);
    }

    public void markProcessed(String eventId, String transportRef) {
        if (eventId != null) {
            cache.put(eventId, transportRef != null && !transportRef.isBlank()
                    ? transportRef
                    : NO_TRANSPORT_REF);
        }
    }

    public void forget(String eventId) {
        if (eventId != null) {
            cache.invalidate(eventId);
        }
    }

    public long size() {
        return cache.estimatedSize();
    }
}
