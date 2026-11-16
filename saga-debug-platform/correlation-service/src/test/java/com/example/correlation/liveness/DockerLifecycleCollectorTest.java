package com.example.correlation.liveness;

import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DockerLifecycleCollectorTest {
    private final List<ServiceLifecycleEvent> stored = new ArrayList<>();
    private final Deque<Object> connections = new ArrayDeque<>();
    private final List<String> requests = new ArrayList<>();
    private DockerLifecycleCollector collector;

    @BeforeEach
    void setUp() {
        ServiceLifecycleEventRepository repository = mock(ServiceLifecycleEventRepository.class);
        when(repository.existsBySourceAndExternalId(anyString(), anyString())).thenAnswer(inv ->
                stored.stream().anyMatch(e -> e.getSource().equals(inv.getArgument(0))
                        && e.getExternalId().equals(inv.getArgument(1))));
        when(repository.save(any())).thenAnswer(inv -> {
            stored.add(inv.getArgument(0));
            return inv.getArgument(0);
        });
        when(repository.findTopBySourceOrderByOccurredAtDesc(anyString())).thenAnswer(inv ->
                stored.stream().max(Comparator.comparing(ServiceLifecycleEvent::getOccurredAt)).map(Optional::of)
                        .orElse(Optional.empty()));

        collector = new DockerLifecycleCollector(repository, new ObjectMapper(), "tcp://proxy:2375",
                "com.docker.compose.service", Duration.ofMinutes(10)) {
            @Override
            Connection open() throws IOException {
                Object next = connections.removeFirst();
                if (next instanceof IOException e) throw e;
                ByteArrayOutputStream out = new ByteArrayOutputStream() {
                    @Override public void flush() {
                        requests.add(toString(StandardCharsets.US_ASCII));
                    }
                };
                ByteArrayInputStream in = new ByteArrayInputStream(((String) next).getBytes(StandardCharsets.UTF_8));
                return new Connection(in, in, out);
            }
        };
    }

    @Test
    void aReconnectResumesFromTheLastReportWithoutLosingOrDuplicating() throws Exception {
        connections.add(stream(die("inventory-service", 1_790_000_100L)));

        connections.add(stream(die("inventory-service", 1_790_000_100L), start("inventory-service", 1_790_000_160L)));

        collector.follow();
        collector.follow();

        assertThat(stored).extracting(e -> e.getServiceName() + ":" + e.getState())
                .containsExactly("inventory-service:DOWN", "inventory-service:UP");
        assertThat(requests.get(1)).contains("since=1790000100");
    }

    @Test
    void anEngineThatIsNotUpYetIsAnErrorToRetryNotAnEmptyStream() {
        connections.add(new IOException("Connection refused"));
        connections.add("HTTP/1.0 502 Bad Gateway\r\n\r\n");

        assertThatThrownBy(() -> collector.follow()).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> collector.follow()).hasMessageContaining("502");
        assertThat(stored).isEmpty();
    }

    private static String stream(String... lines) {
        return "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n" + String.join("\n", lines) + "\n";
    }

    private static String die(String service, long epochSecond) {
        return line("die", service, epochSecond);
    }

    private static String start(String service, long epochSecond) {
        return line("start", service, epochSecond);
    }

    private static String line(String action, String service, long epochSecond) {
        return "{\"Type\":\"container\",\"Action\":\"" + action + "\",\"Actor\":{\"ID\":\"c1\",\"Attributes\":{"
                + "\"com.docker.compose.service\":\"" + service + "\"}},\"timeNano\":" + epochSecond + "000000123}";
    }
}
