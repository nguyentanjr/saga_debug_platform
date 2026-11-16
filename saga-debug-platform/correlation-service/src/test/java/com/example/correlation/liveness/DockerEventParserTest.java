package com.example.correlation.liveness;

import com.example.correlation.domain.ServiceLifecycleEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DockerEventParserTest {
    private final DockerEventParser parser =
            new DockerEventParser(new ObjectMapper(), "com.docker.compose.service");

    @Test
    void anExitIsTheServiceGoingDownWithTheRuntimesExitCode() {
        var event = parser.parse(line("die", "inventory-service", "\"exitCode\":\"143\",")).orElseThrow();

        assertThat(event.getServiceName()).isEqualTo("inventory-service");
        assertThat(event.getState()).isEqualTo(ServiceLifecycleEvent.State.DOWN);
        assertThat(event.getDetail()).isEqualTo("die exitCode=143");
        assertThat(event.getOccurredAt()).isEqualTo(Instant.ofEpochSecond(1790000000L, 123456789L));
        assertThat(event.getExternalId()).isEqualTo("abc123:die:1790000000123456789");
        assertThat(event.getScope()).isEqualTo("saga-debug-platform");
    }

    @Test
    void aStartIsTheServiceComingBack() {
        assertThat(parser.parse(line("start", "inventory-service", "")).orElseThrow().getState())
                .isEqualTo(ServiceLifecycleEvent.State.UP);
    }

    @Test
    void pauseKillAndNetworkEventsAreNotLifecycleFacts() {
        assertThat(parser.parse(line("pause", "payment-service", ""))).isEmpty();
        assertThat(parser.parse(line("kill", "payment-service", "\"signal\":\"19\","))).isEmpty();
        assertThat(parser.parse(
                "{\"Type\":\"network\",\"Action\":\"disconnect\",\"Actor\":{\"ID\":\"n1\",\"Attributes\":{}},"
                        + "\"timeNano\":1790000000123456789}")).isEmpty();
    }

    @Test
    void withoutTheComposeLabelTheContainerNameIsTheService() {
        String noLabel = "{\"Type\":\"container\",\"Action\":\"die\",\"Actor\":{\"ID\":\"abc123\","
                + "\"Attributes\":{\"name\":\"billing\"}},\"timeNano\":1790000000123456789}";
        assertThat(parser.parse(noLabel).orElseThrow().getServiceName()).isEqualTo("billing");
    }

    @Test
    void anythingThatIsNotAnEventObjectIsSkipped() {
        assertThat(parser.parse("HTTP/1.0 200 OK")).isEmpty();
        assertThat(parser.parse("1a3")).isEmpty();
        assertThat(parser.parse("{not json")).isEmpty();
        assertThat(parser.parse(null)).isEmpty();
    }

    private static String line(String action, String service, String extraAttributes) {
        return "{\"status\":\"" + action + "\",\"id\":\"abc123\",\"Type\":\"container\",\"Action\":\""
                + action + "\",\"Actor\":{\"ID\":\"abc123\",\"Attributes\":{" + extraAttributes
                + "\"com.docker.compose.project\":\"saga-debug-platform\","
                + "\"com.docker.compose.service\":\"" + service + "\",\"name\":\"" + service + "\"}},"
                + "\"scope\":\"local\",\"time\":1790000000,\"timeNano\":1790000000123456789}";
    }
}
