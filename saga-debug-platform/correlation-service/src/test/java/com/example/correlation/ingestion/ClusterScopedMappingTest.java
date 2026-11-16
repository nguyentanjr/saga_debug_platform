package com.example.correlation.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

class ClusterScopedMappingTest {
    private static SourceMapping mapping(String name, String pattern, String cluster) {
        SourceMapping mapping = new SourceMapping();
        mapping.setName(name);
        mapping.setTopicPattern(pattern);
        mapping.setCluster(cluster);
        return mapping;
    }

    private static SourceMappingRegistry registryOf(SourceMapping... mappings) {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        for (SourceMapping mapping : mappings) {
            registry.register(mapping, mapping.getName() + ".yaml");
        }
        return registry;
    }

    @Test
    @DisplayName("the same topic name on two clusters reaches two different mappings")
    void sameTopicOnTwoClustersIsTwoSources() {
        SourceMappingRegistry registry = registryOf(
                mapping("eu-orders", "order-created", "retail-eu"),
                mapping("us-orders", "order-created", "logistics-us"));

        assertThat(registry.forTopic("retail-eu", "order-created").getName()).isEqualTo("eu-orders");
        assertThat(registry.forTopic("logistics-us", "order-created").getName()).isEqualTo("us-orders");
    }

    @Test
    @DisplayName("a mapping scoped to one cluster does not claim another's topic")
    void aScopedMappingStaysOnItsCluster() {
        SourceMappingRegistry registry = registryOf(mapping("eu-orders", "order-created", "retail-eu"));

        assertThat(registry.forTopic("logistics-us", "order-created").getName())
                .isNotEqualTo("eu-orders");
    }

    @Test
    @DisplayName("an unscoped mapping reads whatever cluster a record came from")
    void unscopedMappingsAreUnchanged() {
        SourceMappingRegistry registry = registryOf(mapping("any-orders", "order-created", null));

        assertThat(registry.forTopic("retail-eu", "order-created").getName()).isEqualTo("any-orders");
        assertThat(registry.forTopic("logistics-us", "order-created").getName()).isEqualTo("any-orders");
        assertThat(registry.forTopic("order-created").getName()).isEqualTo("any-orders");
    }

    @Test
    @DisplayName("an observation with no origin never matches a cluster-scoped mapping")
    void originlessObservationsDoNotMatchScopedMappings() {
        SourceMappingRegistry registry = registryOf(mapping("eu-orders", "order-created", "retail-eu"));

        assertThat(registry.forTopic(null, "order-created").getName()).isNotEqualTo("eu-orders");
        assertThat(registry.forTopic("order-created").getName()).isNotEqualTo("eu-orders");
    }

    @Test
    @DisplayName("a mapping that named this cluster wins over one that named none")
    void theSpecificDeclarationWins() {
        SourceMappingRegistry unscopedFirst = registryOf(
                mapping("any-orders", "order-.*", null),
                mapping("eu-orders", "order-.*", "retail-eu"));
        SourceMappingRegistry scopedFirst = registryOf(
                mapping("eu-orders", "order-.*", "retail-eu"),
                mapping("any-orders", "order-.*", null));

        assertThat(unscopedFirst.forTopic("retail-eu", "order-created").getName()).isEqualTo("eu-orders");
        assertThat(scopedFirst.forTopic("retail-eu", "order-created").getName()).isEqualTo("eu-orders");

        assertThat(unscopedFirst.forTopic("logistics-us", "order-created").getName()).isEqualTo("any-orders");
        assertThat(scopedFirst.forTopic("logistics-us", "order-created").getName()).isEqualTo("any-orders");
    }

    @Test
    @DisplayName("between two mappings scoped to the same cluster, declaration order decides")
    void declarationOrderStillDecidesAmongEquals() {
        SourceMappingRegistry registry = registryOf(
                mapping("eu-orders", "order-.*", "retail-eu"),
                mapping("eu-orders-too", "order-.*", "retail-eu"));

        assertThat(registry.forTopic("retail-eu", "order-created").getName()).isEqualTo("eu-orders");
    }

    @Test
    @DisplayName("an existing single-cluster deployment resolves exactly as before")
    void singleClusterDeploymentsAreUnaffected() {
        SourceMappingRegistry packaged = new SourceMappingRegistry();
        packaged.load();

        for (String topic : new String[]{"order-created", "payment-completed", "inventory-reserved"}) {
            assertThat(packaged.forTopic(null, topic).getName())
                    .isEqualTo(packaged.forTopic(topic).getName());
        }
    }

    @Test
    @DisplayName("cluster: is read out of the declaration file, not only settable from Java")
    void clusterBindsFromYaml() throws Exception {
        String yaml = """
                source:
                  name: eu-orders
                  cluster: retail-eu
                  topicPattern: "order-.*"
                """;
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        @SuppressWarnings("unchecked")
        Map<String, Object> root = yamlMapper.readValue(yaml, Map.class);
        SourceMapping parsed = yamlMapper.convertValue(root.get("source"), SourceMapping.class);

        assertThat(parsed.getCluster()).isEqualTo("retail-eu");
        assertThat(parsed.matches("retail-eu", "order-created")).isTrue();
        assertThat(parsed.matches("logistics-us", "order-created")).isFalse();
    }

    @Test
    @DisplayName("the cluster travels on the observation, so ingestion can use it")
    void theObservationCarriesItsOrigin() {
        RawObservation fromEu = new RawObservation("order-created", "{}", Instant.now(),
                "order-created-0-1", null, Map.of(), "retail-eu");
        RawObservation fromHttp = new RawObservation("order-created", "{}", Instant.now(),
                null, null, Map.of());

        assertThat(fromEu.origin()).isEqualTo("retail-eu");

        assertThat(fromHttp.origin()).isNull();
    }
}
