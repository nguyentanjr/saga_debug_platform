package com.example.correlation.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaClusterListenersTest {

    private static Binder shippedConfiguration() throws Exception {
        StandardEnvironment env = new StandardEnvironment();
        for (PropertySource<?> source : new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"))) {
            env.getPropertySources().addLast(source);
        }
        return Binder.get(env);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentMessageListenerContainer<String, String> containerOf(KafkaClusterListeners listeners) {
        return (ConcurrentMessageListenerContainer<String, String>) ReflectionTestUtils.getField(listeners, "container");
    }

    @Test
    void theShippedConfigurationBuildsOneConsumerWithTheDeclaredConnectionAndTopics() throws Exception {
        Binder config = shippedConfiguration();
        KafkaProperties kafka = config.bind("spring.kafka", KafkaProperties.class).get();
        KafkaSourceProperties sources = config.bind("correlation.kafka", KafkaSourceProperties.class)
                .orElseGet(KafkaSourceProperties::new);

        ConcurrentMessageListenerContainer<String, String> container = containerOf(
                new KafkaClusterListeners(kafka, sources, mock(SagaEventConsumer.class), mock(CommonErrorHandler.class)));

        assertThat(container.getContainerProperties().getTopicPattern().pattern())
                .isEqualTo("order-.*|payment-.*|inventory-.*|refund-.*");
        assertThat(container.getContainerProperties().getGroupId()).isEqualTo("correlation-engine-group");
        assertThat(container.getConcurrency()).isEqualTo(3);
        assertThat(container.getBeanName()).isEqualTo("saga-events-default");
        assertThat(container.isAutoStartup()).isFalse();

        ConsumerFactory<?, ?> factory = (ConsumerFactory<?, ?>) ReflectionTestUtils.getField(container, "consumerFactory");
        Map<String, Object> consumerConfig = factory.getConfigurationProperties();
        assertThat(consumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        assertThat(consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("correlation-engine-group");
        assertThat(consumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
    }

    @Test
    void concurrencyFallsBackToOneWhenTheListenerDeclaresNone() {
        KafkaProperties kafka = new KafkaProperties();
        kafka.getConsumer().setGroupId("observer");

        ConcurrentMessageListenerContainer<String, String> container = containerOf(
                new KafkaClusterListeners(kafka, new KafkaSourceProperties(),
                        mock(SagaEventConsumer.class), mock(CommonErrorHandler.class)));

        assertThat(container.getConcurrency()).isEqualTo(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void everyRecordTheContainerReceivesIsHandedToTheConsumer() {
        SagaEventConsumer consumer = mock(SagaEventConsumer.class);
        ConcurrentMessageListenerContainer<String, String> container = containerOf(
                new KafkaClusterListeners(new KafkaProperties(), new KafkaSourceProperties(),
                        consumer, mock(CommonErrorHandler.class)));
        ConsumerRecord<String, String> record = new ConsumerRecord<>("order-events", 0, 7L, "k", "{}");

        ((MessageListener<String, String>) container.getContainerProperties().getMessageListener()).onMessage(record);

        verify(consumer).handleEvent(record);
    }
}
