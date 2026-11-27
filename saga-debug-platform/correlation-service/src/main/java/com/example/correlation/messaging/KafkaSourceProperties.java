package com.example.correlation.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "correlation.kafka")
public class KafkaSourceProperties {
    private String topicPattern = "order-.*|payment-.*|inventory-.*|refund-.*";

    public String getTopicPattern() { return topicPattern; }
    public void setTopicPattern(String topicPattern) { this.topicPattern = topicPattern; }
}
