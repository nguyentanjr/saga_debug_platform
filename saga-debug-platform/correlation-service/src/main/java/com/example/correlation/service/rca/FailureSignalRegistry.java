package com.example.correlation.service.rca;

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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Component
public class FailureSignalRegistry {
    private static final Logger log = LoggerFactory.getLogger(FailureSignalRegistry.class);

    private final String declarationsDirectory;

    private final Map<String, FailureSignalDeclaration> bySource = new LinkedHashMap<>();

    public FailureSignalRegistry(
            @Value("${correlation.declarations.dir:}") String declarationsDirectory) {
        this.declarationsDirectory = declarationsDirectory;
    }

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            List<Resource> resources = DeclarationFiles.find("failure-signals", declarationsDirectory);
            for (Resource resource : resources) {
                try (InputStream is = resource.getInputStream()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                    Object declared = root.get("failureSignals");
                    if (declared == null) {
                        log.error("Failure-signal file {} has no 'failureSignals' key; ignored",
                                resource.getFilename());
                        continue;
                    }
                    FailureSignalDeclaration declaration =
                            yamlMapper.convertValue(declared, FailureSignalDeclaration.class);
                    register(declaration, resource.getFilename());
                } catch (Exception e) {
                    log.error("Failed to load failure-signal file {}: {}",
                            resource.getFilename(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to scan for failure-signal files: {}", e.getMessage(), e);
        }

        if (bySource.isEmpty()) {
            log.debug("No failure-signal declarations. Rules that read a source's failure "
                    + "text produce no candidate, which is the intended behaviour when the "
                    + "platform has not been told how that source words its failures.");
        } else {
            log.info("Loaded failure-signal declaration(s) for source(s): {}", bySource.keySet());
        }
    }

    private void register(FailureSignalDeclaration declaration, String filename) {
        if (declaration == null || declaration.getSource() == null
                || declaration.getSource().isBlank()) {
            log.error("Failure-signal file {} declares no source; ignored", filename);
            return;
        }
        FailureSignalDeclaration existing = bySource.get(declaration.getSource());
        if (existing != null) {
            log.error("Source '{}' already has a failure-signal declaration; {} ignored. "
                    + "Two declarations for one source is a deployment error, and choosing "
                    + "between them by file order would make a diagnosis depend on a "
                    + "directory listing.", declaration.getSource(), filename);
            return;
        }
        bySource.put(declaration.getSource(), declaration);
    }

    public Optional<FailureSignalDeclaration> forSource(String sourceName) {
        if (sourceName == null) return Optional.empty();
        return Optional.ofNullable(bySource.get(sourceName));
    }

    public Set<String> declaredSources() {
        return Collections.unmodifiableSet(bySource.keySet());
    }
}
