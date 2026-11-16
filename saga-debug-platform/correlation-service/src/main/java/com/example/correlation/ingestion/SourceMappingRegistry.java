package com.example.correlation.ingestion;

import com.example.correlation.config.DeclarationFiles;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class SourceMappingRegistry {
    private static final Logger log = LoggerFactory.getLogger(SourceMappingRegistry.class);

    @Value("${correlation.declarations.dir:}")
    private String declarationsDirectory;

    private final List<SourceMapping> mappings = new ArrayList<>();
    private final SourceMapping identity = SourceMapping.identity();

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            List<Resource> resources = DeclarationFiles.find("ingestion", declarationsDirectory);

            for (Resource resource : resources) {
                try (InputStream is = resource.getInputStream()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                    SourceMapping mapping =
                            yamlMapper.convertValue(root.get("source"), SourceMapping.class);
                    register(mapping, resource.getFilename());
                } catch (Exception e) {
                    log.error("Failed to load source mapping {}: {}",
                            resource.getFilename(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to scan for source mappings: {}", e.getMessage(), e);
        }

        log.info("Loaded {} source mapping(s): {}", mappings.size(),
                mappings.stream().map(SourceMapping::getName).toList());
    }

    void register(SourceMapping mapping, String filename) {
        if (mapping == null || mapping.getName() == null) {
            log.error("Source mapping {} has no name; ignored", filename);
            return;
        }
        if (mapping.systemName().contains(com.example.correlation.domain.ExecutionKey.SEPARATOR)) {
            log.error("Source mapping '{}' observes system '{}', which contains '{}' — "
                            + "that sequence is reserved for namespacing; ignored",
                    mapping.getName(), mapping.systemName(),
                    com.example.correlation.domain.ExecutionKey.SEPARATOR);
            return;
        }
        if (mapping.getTopicPattern() == null) {
            log.error("Source mapping '{}' declares no topicPattern; it could never match",
                    mapping.getName());
            return;
        }

        mappings.stream()
                .filter(existing -> existing.getName().equals(mapping.getName()))
                .findFirst()
                .ifPresent(existing -> log.error(
                        "Source mapping '{}' is declared more than once ({} is a duplicate). "
                                + "Both are registered and the first whose topicPattern matches "
                                + "is used, so one of them may never be read. Give each file its "
                                + "own name, and use 'system:' if they observe one system.",
                        mapping.getName(), filename));
        mappings.add(mapping);
        if (mapping.getName().equals(mapping.systemName())) {
            log.info("Source '{}' reads topics matching {} — correlation key at '{}'",
                    mapping.getName(), mapping.getTopicPattern(),
                    SourceMapping.describe(mapping.getFields().getExecutionId()));
        } else {
            log.info("Source '{}' reads topics matching {} for observed system '{}' — "
                            + "correlation key at '{}'",
                    mapping.getName(), mapping.getTopicPattern(), mapping.systemName(),
                    SourceMapping.describe(mapping.getFields().getExecutionId()));
        }
    }

    public SourceMapping forTopic(String topic) {
        return mappings.stream()
                .filter(mapping -> mapping.matches(null, topic))
                .findFirst()
                .orElse(identity);
    }

    public SourceMapping forTopic(String origin, String topic) {
        if (origin == null || origin.isBlank()) return forTopic(topic);

        return mappings.stream()
                .filter(mapping -> origin.equals(mapping.getCluster()) && mapping.matches(topic))
                .findFirst()
                .orElseGet(() -> mappings.stream()
                        .filter(mapping -> mapping.matches(origin, topic))
                        .findFirst()
                        .orElse(identity));
    }

    public List<String> sourceNames() {
        return mappings.stream().map(SourceMapping::getName).toList();
    }

    public Set<String> observedSystems() {
        return mappings.stream()
                .map(SourceMapping::systemName)
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    }

    public Set<String> knownSourceNames() {
        Set<String> names = new LinkedHashSet<>();
        names.add(identity.getName());
        for (SourceMapping mapping : mappings) {
            names.add(mapping.systemName());
            names.add(mapping.aliasNamespace());
        }
        names.remove(null);
        return names;
    }
}
