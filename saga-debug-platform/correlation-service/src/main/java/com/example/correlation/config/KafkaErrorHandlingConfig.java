package com.example.correlation.config;

import com.example.correlation.ingestion.SourceMappingRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.ExponentialBackOff;

import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

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
                JsonProcessingException.class,
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
        Throwable current = exception;
        while (current != null) {
            if (isStoreOutage(current)) return true;
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return false;
    }

    private static boolean isStoreOutage(Throwable exception) {
        return exception instanceof TransientDataAccessException
                || exception instanceof DataAccessResourceFailureException
                || exception instanceof CannotCreateTransactionException
                || exception instanceof SQLTransientException
                || exception instanceof SQLRecoverableException;
    }

    static String valueForLog(ConsumerRecord<?, ?> record, SourceMappingRegistry mappings) {
        if (record.value() == null) return null;
        return mappings.forTopic(record.topic()).redact(String.valueOf(record.value()));
    }
}
