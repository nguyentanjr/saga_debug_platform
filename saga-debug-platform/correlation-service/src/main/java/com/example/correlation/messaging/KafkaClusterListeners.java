package com.example.correlation.messaging;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();
    private final List<ConsumerGroup> groups = new ArrayList<>();
    private volatile boolean running;

    public KafkaClusterListeners(KafkaProperties kafkaProperties,
                                 KafkaSourceProperties sourceProperties,
                                 SagaEventConsumer consumer,
                                 CommonErrorHandler errorHandler) {
        for (KafkaSourceProperties.Cluster cluster : declaredClusters(kafkaProperties, sourceProperties)) {
            containers.add(containerFor(kafkaProperties, cluster, consumer, errorHandler));
            groups.add(groupFor(kafkaProperties, cluster));
        }
    }

    private static List<KafkaSourceProperties.Cluster> declaredClusters(
            KafkaProperties kafkaProperties, KafkaSourceProperties sourceProperties) {
        if (!sourceProperties.declaresClusters()) {
            KafkaSourceProperties.Cluster only = new KafkaSourceProperties.Cluster();
            only.setBootstrapServers(String.join(",", kafkaProperties.getBootstrapServers()));
            only.setTopicPattern(sourceProperties.getTopicPattern());
            log.info("Reading one Kafka cluster at {} for topics matching {}",
                    only.getBootstrapServers(), only.getTopicPattern());
            return List.of(only);
        }

        List<KafkaSourceProperties.Cluster> usable = new ArrayList<>();
        Set<String> namesSeen = new HashSet<>();
        for (KafkaSourceProperties.Cluster cluster : sourceProperties.getClusters()) {
            List<String> missing = cluster.missingFields();
            if (!missing.isEmpty()) {
                log.error("Kafka cluster declaration is missing {} and will not be read: {}",
                        missing, cluster.getName() != null ? cluster.getName() : "(unnamed)");
                continue;
            }

            if (!namesSeen.add(cluster.getName())) {
                log.error("Kafka cluster '{}' is declared more than once; the second "
                        + "declaration is ignored. A mapping scoped to that name could not "
                        + "say which of them it meant.", cluster.getName());
                continue;
            }
            usable.add(cluster);
            log.info("Reading Kafka cluster '{}' at {} for topics matching {}",
                    cluster.getName(), cluster.getBootstrapServers(), cluster.getTopicPattern());
        }
        if (usable.isEmpty()) {
            log.error("Every declared Kafka cluster was unusable; this deployment reads no "
                    + "Kafka telemetry at all. HTTP ingestion is unaffected.");
        }
        return usable;
    }

    private static ConsumerGroup groupFor(KafkaProperties kafkaProperties, KafkaSourceProperties.Cluster cluster) {
        Map<String, Object> admin = kafkaProperties.buildAdminProperties(null);
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        String groupId = cluster.getGroupId() != null && !cluster.getGroupId().isBlank()
                ? cluster.getGroupId() : kafkaProperties.getConsumer().getGroupId();
        return new ConsumerGroup(admin, groupId);
    }

    private static ConcurrentMessageListenerContainer<String, String> containerFor(
            KafkaProperties kafkaProperties, KafkaSourceProperties.Cluster cluster,
            SagaEventConsumer consumer, CommonErrorHandler errorHandler) {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        if (cluster.getGroupId() != null && !cluster.getGroupId().isBlank()) {
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, cluster.getGroupId());
        }
        if (cluster.getAutoOffsetReset() != null && !cluster.getAutoOffsetReset().isBlank()) {
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cluster.getAutoOffsetReset());
        }

        ContainerProperties containerProperties =
                new ContainerProperties(Pattern.compile(cluster.getTopicPattern()));
        String origin = cluster.getName();
        containerProperties.setMessageListener(
                (MessageListener<String, String>) (ConsumerRecord<String, String> record) ->
                        consumer.handle(record, origin));
        containerProperties.setGroupId(
                consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG) instanceof String id
                        ? id : null);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(
                        new DefaultKafkaConsumerFactory<>(consumerProperties), containerProperties);
        container.setConcurrency(cluster.getConcurrency() != null
                ? cluster.getConcurrency()
                : kafkaProperties.getListener().getConcurrency() != null
                        ? kafkaProperties.getListener().getConcurrency()
                        : 1);
        container.setCommonErrorHandler(errorHandler);
        container.setBeanName("saga-events-" + (origin != null ? origin : "default"));
        container.setAutoStartup(false);
        return container;
    }

    public Long backlog() {
        Long total = null;
        for (ConsumerGroup group : groups) {
            Long lag = group.backlog();
            if (lag != null) total = (total == null ? 0 : total) + lag;
        }
        return total;
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

    public int containerCount() {
        return containers.size();
    }

    @Override
    public void start() {
        containers.forEach(ConcurrentMessageListenerContainer::start);
        running = true;
    }

    @Override
    public void stop() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        groups.forEach(ConsumerGroup::close);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
