package com.example.correlation.ingestion;

import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class DeclaredVocabularyTest {
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    @Test
    void everyMappedEventNameIsKnownToItsSystemsWorkflow() throws Exception {
        WorkflowRegistry workflows = new WorkflowRegistry();
        ReflectionTestUtils.setField(workflows, "declarationsDirectory", declarations().toString());
        workflows.load();

        List<String> unknown = new ArrayList<>();
        TreeSet<String> checked = new TreeSet<>();
        File[] mappings = declarations().resolve("ingestion").toFile().listFiles((dir, name) -> name.endsWith(".yaml"));
        assertThat(mappings).isNotEmpty();
        for (File file : mappings) {
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) YAML.readValue(file, Map.class).get("source");
            List<WorkflowSpec> candidates = workflowsOf(source, workflows);
            if (candidates.isEmpty()) continue;
            candidates.forEach(spec -> checked.add(spec.getName()));
            @SuppressWarnings("unchecked")
            Map<String, String> eventTypes = (Map<String, String>) source.getOrDefault("eventTypes", Map.of());
            for (String name : new TreeSet<>(eventTypes.values())) {
                if (candidates.stream().allMatch(spec -> spec.classifyEvent(name).isEmpty())) {
                    unknown.add(file.getName() + ": " + name + " not in "
                            + candidates.stream().map(WorkflowSpec::getName).toList());
                }
            }
        }

        assertThat(checked).as("systems covered").contains("KMS_ORDER_SAGA", "SPM_ORDER_SAGA");
        assertThat(unknown).isEmpty();
    }

    private static List<WorkflowSpec> workflowsOf(Map<String, Object> source, WorkflowRegistry workflows) {
        Object declared = source.get("workflow");
        if (declared != null) return workflows.byType(declared.toString()).stream().toList();
        Object system = source.get("system");
        if (system == null) return List.of();
        return workflows.workflowNames().stream()
                .map(workflows::byType).flatMap(Optional::stream)
                .filter(spec -> spec.getSources().contains(system.toString()))
                .toList();
    }

    private static Path declarations() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        return root.resolve("declarations");
    }
}
