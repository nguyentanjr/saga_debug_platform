package com.example.correlation.messaging;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaClusterListeners implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(KafkaClusterListeners.class);

    private final ConcurrentMessageListenerContainer<String, String> container;
    private final ConsumerGroup group;
    private volatile boolean running;

    public KafkaClusterListeners(KafkaProperties kafkaProperties,
                                 KafkaSourceProperties sourceProperties,
                                 SagaEventConsumer consumer,
                                 CommonErrorHandler errorHandler) {
        String bootstrapServers = String.join(",", kafkaProperties.getBootstrapServers());
        String topicPattern = sourceProperties.getTopicPattern();
        log.info("Reading Kafka at {} for topics matching {}", bootstrapServers, topicPattern);
        this.container = containerFor(kafkaProperties, bootstrapServers, topicPattern, consumer, errorHandler);
        this.group = groupFor(kafkaProperties, bootstrapServers);
    }

    private static ConsumerGroup groupFor(KafkaProperties kafkaProperties, String bootstrapServers) {
        Map<String, Object> admin = kafkaProperties.buildAdminProperties(null);
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new ConsumerGroup(admin, kafkaProperties.getConsumer().getGroupId());
    }

    private static ConcurrentMessageListenerContainer<String, String> containerFor(
            KafkaProperties kafkaProperties, String bootstrapServers, String topicPattern,
            SagaEventConsumer consumer, CommonErrorHandler errorHandler) {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties(null);
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        ContainerProperties containerProperties = new ContainerProperties(Pattern.compile(topicPattern));
        containerProperties.setMessageListener(
                (MessageListener<String, String>) (ConsumerRecord<String, String> record) ->
                        consumer.handleEvent(record));
        containerProperties.setGroupId(
                consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG) instanceof String id
                        ? id : null);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(
                        new DefaultKafkaConsumerFactory<>(consumerProperties), containerProperties);
        container.setConcurrency(concurrencyOf(kafkaProperties));
        container.setCommonErrorHandler(errorHandler);
        container.setBeanName("saga-events-default");
        container.setAutoStartup(false);
        return container;
    }

    private static int concurrencyOf(KafkaProperties kafkaProperties) {
        Integer listenerDefault = kafkaProperties.getListener().getConcurrency();
        return listenerDefault != null ? listenerDefault : 1;
    }

    public Long backlog() {
        return group.backlog();
    }

    static Long backlogOf(Map<TopicPartition, Long> committed, Map<TopicPartition, Long> ends) {
        if (committed.isEmpty()) return null;
        long total = 0;
        for (Map.Entry<TopicPartition, Long> entry : committed.entrySet()) {
            Long end = ends.get(entry.getKey());
            if (end != null) total += Math.max(0, end - entry.getValue());
        }
        return total;
    }

    private static final class ConsumerGroup {
        private final Map<String, Object> adminProperties;
        private final String groupId;
        private Admin admin;

        ConsumerGroup(Map<String, Object> adminProperties, String groupId) {
            this.adminProperties = adminProperties;
            this.groupId = groupId;
        }

        synchronized Long backlog() {
            if (groupId == null) return null;
            try {
                if (admin == null) admin = Admin.create(adminProperties);
                Map<TopicPartition, Long> committed = new HashMap<>();
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS)
                        .forEach((tp, om) -> { if (om != null) committed.put(tp, om.offset()); });
                if (committed.isEmpty()) return null;
                Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
                committed.keySet().forEach(tp -> latest.put(tp, OffsetSpec.latest()));
                Map<TopicPartition, Long> ends = new HashMap<>();
                admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS)
                        .forEach((tp, info) -> ends.put(tp, info.offset()));
                return backlogOf(committed, ends);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                log.debug("Could not measure backlog of group {}: {}", groupId, e.toString());
                return null;
            }
        }

        synchronized void close() {
            if (admin != null) admin.close(Duration.ofSeconds(5));
            admin = null;
        }
    }

    @Override
    public void start() {
        container.start();
        running = true;
    }

    @Override
    public void stop() {
        container.stop();
        group.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
