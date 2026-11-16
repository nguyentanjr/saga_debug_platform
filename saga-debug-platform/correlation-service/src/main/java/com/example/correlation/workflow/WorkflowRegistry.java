package com.example.correlation.workflow;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class WorkflowRegistry {
    private static final Logger log = LoggerFactory.getLogger(WorkflowRegistry.class);

    @Value("${correlation.declarations.dir:}")
    private String declarationsDirectory;

    private final Map<String, WorkflowSpec> byName = new LinkedHashMap<>();

    private final Map<String, List<WorkflowSpec>> byStartEvent = new LinkedHashMap<>();

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            List<Resource> resources = DeclarationFiles.find("workflows", declarationsDirectory);

            for (Resource resource : resources) {
                try (InputStream is = resource.getInputStream()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                    WorkflowSpec spec = yamlMapper.convertValue(root.get("workflow"), WorkflowSpec.class);
                    register(spec, resource.getFilename());
                } catch (Exception e) {
                    log.error("Failed to load workflow spec {}: {}", resource.getFilename(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to scan for workflow specs: {}", e.getMessage(), e);
        }

        if (byName.isEmpty()) {
            log.error("No workflow specifications loaded. Every execution will be treated "
                    + "as unrecognised and no spec-driven detection will run.");
        } else {
            log.info("Loaded {} workflow spec(s): {}", byName.size(), byName.keySet());
        }
    }

    void register(WorkflowSpec spec, String filename) {
        if (spec == null || spec.getName() == null) {
            log.error("Workflow spec {} has no name; ignored", filename);
            return;
        }

        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(spec);
        report.warnings().forEach(warning ->
                log.warn("Workflow '{}' ({}): {}", spec.getName(), filename, warning));
        if (!report.isUsable()) {
            report.errors().forEach(error ->
                    log.error("Workflow '{}' ({}) rejected: {}", spec.getName(), filename, error));
            return;
        }

        WorkflowSpec previous = byName.putIfAbsent(spec.getName(), spec);
        if (previous != null) {
            log.error("Duplicate workflow name '{}' in {}; keeping the one already loaded",
                    spec.getName(), filename);
            return;
        }

        for (String eventType : spec.startEventTypes()) {
            byStartEvent.computeIfAbsent(eventType, k -> new ArrayList<>()).add(spec);
        }

        log.info("Workflow '{}' v{} — {} step(s), start event(s) {}{}",
                spec.getName(), spec.getVersion(),
                spec.getSteps() != null ? spec.getSteps().size() : 0,
                spec.startEventTypes(),
                report.isClean() ? "" : " (" + report.warnings().size() + " warning(s))");
    }

    public Optional<WorkflowSpec> byType(String workflowType) {
        return Optional.ofNullable(workflowType).map(byName::get);
    }

    public Optional<WorkflowSpec> forStartEvent(String eventType) {
        return unique(candidatesFor(eventType), eventType, null);
    }

    public Optional<WorkflowSpec> forStartEvent(String eventType, String sourceName) {
        List<WorkflowSpec> scoped = candidatesFor(eventType).stream()
                .filter(spec -> spec.describesSource(sourceName))
                .toList();
        return unique(scoped, eventType, sourceName);
    }

    private List<WorkflowSpec> candidatesFor(String eventType) {
        List<WorkflowSpec> candidates = byStartEvent.get(eventType);
        return candidates == null ? List.of() : candidates;
    }

    private Optional<WorkflowSpec> unique(List<WorkflowSpec> candidates,
            String eventType, String sourceName) {
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        if (candidates.size() > 1) {
            log.warn("Event type '{}'{} starts more than one workflow ({}); cannot pick one. "
                            + "Declare 'sources:' on the specifications, or 'workflow:' on the "
                            + "source mapping.",
                    eventType,
                    sourceName != null ? " from source '" + sourceName + "'" : "",
                    candidates.stream().map(WorkflowSpec::getName).toList());
            return Optional.empty();
        }
        return Optional.of(candidates.get(0));
    }

    public Optional<WorkflowSpec> resolve(String workflowType, String declaredWorkflow,
            String sourceName, String eventType) {
        Optional<WorkflowSpec> explicit = byType(workflowType);
        if (explicit.isPresent()) {
            return explicit;
        }
        return resolveFromEvidence(declaredWorkflow, sourceName, eventType);
    }

    public Optional<WorkflowSpec> resolveFromEvidence(String declaredWorkflow,
            String sourceName, String eventType) {
        Optional<WorkflowSpec> sourceScoped = forStartEvent(eventType, sourceName)
                .filter(WorkflowSpec::isSourceScoped);
        Optional<WorkflowSpec> mappingDeclared = byType(declaredWorkflow);

        if (sourceScoped.isPresent()) {
            if (mappingDeclared.isPresent()
                    && !mappingDeclared.get().getName().equals(sourceScoped.get().getName())) {
                log.error("Source '{}' is mapped to workflow '{}', but specification '{}' "
                                + "declares that source and starts on '{}'. Using '{}'; fix "
                                + "whichever declaration is wrong.",
                        sourceName, mappingDeclared.get().getName(),
                        sourceScoped.get().getName(), eventType, sourceScoped.get().getName());
            }
            return sourceScoped;
        }
        if (mappingDeclared.isPresent()) {
            return mappingDeclared;
        }
        return forStartEvent(eventType, sourceName).or(() -> forStartEvent(eventType));
    }

    @Deprecated
    public Optional<WorkflowSpec> resolve(String workflowType, String eventType) {
        return resolve(workflowType, null, null, eventType);
    }

    public List<String> workflowNames() {
        return List.copyOf(byName.keySet());
    }
}
