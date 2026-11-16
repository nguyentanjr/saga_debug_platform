package com.example.correlation.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TopicDiscoveryIntervalTest {
    @Test
    void theShippedConsumerConfigurationRefreshesTopicMetadataWithinAMinute() throws Exception {
        StandardEnvironment env = new StandardEnvironment();
        MutablePropertySources sources = env.getPropertySources();
        for (PropertySource<?> source : new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"))) {
            sources.addLast(source);
        }
        KafkaProperties kafka = Binder.get(env).bind("spring.kafka", KafkaProperties.class).get();

        Object configured = kafka.buildConsumerProperties(null).get(ConsumerConfig.METADATA_MAX_AGE_CONFIG);

        assertThat(configured).isNotNull();
        assertThat(Duration.ofMillis(Long.parseLong(configured.toString())))
                .isLessThanOrEqualTo(Duration.ofMinutes(1));
    }
}
