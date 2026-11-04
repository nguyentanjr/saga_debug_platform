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

/**
 * L19: every event name a shipped mapping produces is one its system's workflow knows.
 *
 * <p>Read from {@code declarations/}, the files the observer loads, for every mapping whose
 * workflow is established — named by the mapping ({@code workflow:}) or claiming the
 * mapping's system ({@code sources:}). An unknown name lands in no step, and the run is
 * judged without it, silently; before this only the hotel system (OutboxPatternIngestionTest)
 * was checked, so a rename on one side of kms or spm would surface only in a live run.
 * Mappings with no established workflow — probes, and the hotel system whose specification
 * does not declare its sources — are out of scope here.
 */
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

    /**
     * The workflow the mapping names, or every workflow claiming its system: one system may run
     * several (ewallet-core-transfer: W2N and W2O share topics), and a name need only be known
     * to one of them.
     */
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

    /** The repository's declarations directory, whether surefire runs from the module or the root. */
    private static Path declarations() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        return root.resolve("declarations");
    }
}
