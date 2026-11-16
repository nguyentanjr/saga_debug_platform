package com.example.correlation.messaging;

import java.util.function.Supplier;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

@Component("ingestionBacklog")
public class IngestionBacklogHealth implements HealthIndicator {
    static final Status LAGGING = new Status("LAGGING",
            "Ingestion is behind; recent findings may reflect events not yet read");

    private final Supplier<Long> backlog;
    private final long threshold;

    @Autowired
    public IngestionBacklogHealth(KafkaClusterListeners listeners, MeterRegistry meters,
                                  @Value("${correlation.kafka.backlog-warning-threshold:10000}") long threshold) {
        this(listeners::backlog, threshold);
        Gauge.builder("correlation.kafka.backlog", () -> {
                    Long value = backlog.get();
                    return value == null ? Double.NaN : value;
                })
                .description("Records published on observed topics but not yet read")
                .register(meters);
    }

    IngestionBacklogHealth(Supplier<Long> backlog, long threshold) {
        this.backlog = backlog;
        this.threshold = threshold;
    }

    @Override
    public Health health() {
        Long value = backlog.get();
        if (value == null) {
            return Health.unknown().withDetail("reason", "no committed offsets could be read").build();
        }
        return Health.status(value > threshold ? LAGGING : Status.UP)
                .withDetail("backlog", value)
                .withDetail("threshold", threshold)
                .build();
    }
}
