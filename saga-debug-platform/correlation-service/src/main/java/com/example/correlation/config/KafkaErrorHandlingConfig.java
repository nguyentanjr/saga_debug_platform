package com.example.correlation.config;

import com.example.correlation.ingestion.SourceMappingRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaErrorHandlingConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(SourceMappingRegistry mappings) {
        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) ->
                log.error("Giving up on record after retries — topic={} partition={} offset={} "
                                + "key={} value={}. This event is NOT stored; replay it manually "
                                + "if the saga matters.",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), valueForLog(record, mappings), exception),
                bounded());

        handler.addNotRetryableExceptions(
                com.fasterxml.jackson.core.JsonProcessingException.class,
                IllegalArgumentException.class);

        handler.setBackOffFunction((record, exception) -> {
            if (storeUnavailable(exception)) {
                log.warn("Observer store unavailable; holding topic={} partition={} offset={} "
                                + "until it returns: {}",
                        record.topic(), record.partition(), record.offset(), exception.toString());
            }
            return backOffFor(exception);
        });

        return handler;
    }

    private static final long MAX_INTERVAL_MS = 20_000L;

    static ExponentialBackOff backOffFor(Exception exception) {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(MAX_INTERVAL_MS);
        if (!storeUnavailable(exception)) backOff.setMaxElapsedTime(60_000L);
        return backOff;
    }

    private static ExponentialBackOff bounded() {
        return backOffFor(new RuntimeException());
    }

    static boolean storeUnavailable(Throwable exception) {
        for (Throwable t = exception; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof org.springframework.dao.TransientDataAccessException
                    || t instanceof org.springframework.dao.DataAccessResourceFailureException
                    || t instanceof org.springframework.transaction.CannotCreateTransactionException
                    || t instanceof java.sql.SQLTransientException
                    || t instanceof java.sql.SQLRecoverableException) {
                return true;
            }
        }
        return false;
    }

    static String valueForLog(ConsumerRecord<?, ?> record, SourceMappingRegistry mappings) {
        if (record.value() == null) return null;
        return mappings.forTopic(record.topic()).redact(String.valueOf(record.value()));
    }
}
