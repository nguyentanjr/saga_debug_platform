package com.example.correlation.config;

import com.example.correlation.ingestion.SourceMapping;
import com.example.correlation.ingestion.SourceMappingRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.BackOffExecution;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaErrorHandlingConfigTest {
    @Test
    void aGivenUpRecordIsLoggedWithItsDeclaredFieldsRedacted() {
        SourceMapping mapping = new SourceMapping();
        mapping.setName("payments");
        mapping.setTopicPattern("payment[.]inbox[.]events");
        mapping.setRedact(List.of("creditCardNo"));
        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mapping.matches(topic) ? mapping : SourceMapping.identity();
            }
        };

        String logged = KafkaErrorHandlingConfig.valueForLog(new ConsumerRecord<>("payment.inbox.events", 0, 7L,
                "SAGA-1", "{\"type\":\"REQUEST\",\"creditCardNo\":\"4111111111111111\"}"), registry);

        assertThat(logged).doesNotContain("4111111111111111").contains("\"type\":\"REQUEST\"");
        assertThat(KafkaErrorHandlingConfig.valueForLog(
                new ConsumerRecord<>("other", 0, 1L, "k", "{\"creditCardNo\":\"x\"}"), registry))
                .isEqualTo("{\"creditCardNo\":\"x\"}");
    }

    @Test
    void storeOutagesAreToldApartFromFaultsInTheRecord() {
        assertThat(KafkaErrorHandlingConfig.storeUnavailable(new ListenerExecutionFailedException("wrapped",
                new CannotCreateTransactionException("Could not open JPA EntityManager")))).isTrue();
        assertThat(KafkaErrorHandlingConfig.storeUnavailable(
                new OptimisticLockingFailureException("lost the race"))).isTrue();
        assertThat(KafkaErrorHandlingConfig.storeUnavailable(new RuntimeException(
                new java.sql.SQLTransientConnectionException("pool timeout")))).isTrue();

        assertThat(KafkaErrorHandlingConfig.storeUnavailable(
                new DataIntegrityViolationException("constraint"))).isFalse();
        assertThat(KafkaErrorHandlingConfig.storeUnavailable(new NullPointerException())).isFalse();
    }

    @Test
    void aRecordIsHeldForAsLongAsTheStoreIsDownButAFaultyOneIsLetGo() {
        BackOffExecution outage = KafkaErrorHandlingConfig.backOffFor(new ListenerExecutionFailedException(
                "listener failed", new CannotCreateTransactionException("Connection refused"))).start();
        long waited = 0;
        for (int i = 0; i < 10_000; i++) {
            long next = outage.nextBackOff();
            assertThat(next).isNotEqualTo(BackOffExecution.STOP);
            waited += next;
        }
        assertThat(waited).isGreaterThan(60_000L * 60 * 24);

        BackOffExecution fault = KafkaErrorHandlingConfig.backOffFor(new IllegalStateException("bug")).start();
        long spent = 0;
        long next;
        while ((next = fault.nextBackOff()) != BackOffExecution.STOP) spent += next;
        assertThat(spent).isBetween(30_000L, 80_000L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void aRecordFailingOnAnUnreachableStoreIsSeekedBackNotGivenUp() {
        DefaultErrorHandler handler = new KafkaErrorHandlingConfig()
                .kafkaErrorHandler(new SourceMappingRegistry());
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("order-created", 0, 42L, "SAGA-1", "{}");
        Consumer<String, String> consumer = mock(Consumer.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(true);

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> handler.handleRemaining(new ListenerExecutionFailedException(
                            "listener failed", new CannotCreateTransactionException("Connection refused")),
                    List.of(record), consumer, container))
                    .hasMessageContaining("Seek to current");
        }
        verify(consumer, times(3)).seek(eq(new TopicPartition("order-created", 0)), eq(42L));

        handler.handleRemaining(new ListenerExecutionFailedException("listener failed",
                        new IllegalArgumentException("bad record")),
                List.of(new ConsumerRecord<>("order-created", 0, 43L, "SAGA-2", "{}")), consumer, container);
        verify(consumer, times(0)).seek(eq(new TopicPartition("order-created", 0)), eq(43L));
    }
}
