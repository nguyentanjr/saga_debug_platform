package com.example.correlation.messaging;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "correlation.kafka")
public class KafkaSourceProperties {
    private String topicPattern = "order-.*|payment-.*|inventory-.*|refund-.*";

    private List<Cluster> clusters = new ArrayList<>();

    public String getTopicPattern() { return topicPattern; }
    public void setTopicPattern(String topicPattern) { this.topicPattern = topicPattern; }

    public List<Cluster> getClusters() { return clusters; }
    public void setClusters(List<Cluster> clusters) {
        this.clusters = clusters != null ? clusters : new ArrayList<>();
    }

    public boolean declaresClusters() {
        return !clusters.isEmpty();
    }

    public static class Cluster {
        private String name;

        private String bootstrapServers;

        private String topicPattern;

        private String groupId;

        private Integer concurrency;

        private String autoOffsetReset;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

        public String getTopicPattern() { return topicPattern; }
        public void setTopicPattern(String topicPattern) { this.topicPattern = topicPattern; }

        public String getGroupId() { return groupId; }
        public void setGroupId(String groupId) { this.groupId = groupId; }

        public Integer getConcurrency() { return concurrency; }
        public void setConcurrency(Integer concurrency) { this.concurrency = concurrency; }

        public String getAutoOffsetReset() { return autoOffsetReset; }
        public void setAutoOffsetReset(String autoOffsetReset) { this.autoOffsetReset = autoOffsetReset; }

        public List<String> missingFields() {
            List<String> missing = new ArrayList<>();
            if (name == null || name.isBlank()) missing.add("name");
            if (bootstrapServers == null || bootstrapServers.isBlank()) missing.add("bootstrap-servers");
            if (topicPattern == null || topicPattern.isBlank()) missing.add("topic-pattern");
            return missing;
        }
    }
}
