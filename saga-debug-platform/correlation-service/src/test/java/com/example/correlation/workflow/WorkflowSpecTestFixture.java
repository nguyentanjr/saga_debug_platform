package com.example.correlation.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.util.Map;

public final class WorkflowSpecTestFixture {
    private WorkflowSpecTestFixture() {
    }

    public static WorkflowSpec orderSaga() {
        return load("workflows/order-saga-workflow.yaml");
    }

    public static WorkflowRegistry registryOf(WorkflowSpec... specs) {
        WorkflowRegistry registry = new WorkflowRegistry();
        for (WorkflowSpec spec : specs) registry.register(spec, spec.getName() + ".yaml");
        return registry;
    }

    @SuppressWarnings("unchecked")
    public static WorkflowSpec load(String classpathResource) {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try (InputStream is = WorkflowSpecTestFixture.class.getClassLoader()
                .getResourceAsStream(classpathResource)) {
            if (is == null) {
                throw new IllegalStateException("Workflow spec not found on classpath: " + classpathResource);
            }
            Map<String, Object> root = yamlMapper.readValue(is, Map.class);
            return yamlMapper.convertValue(root.get("workflow"), WorkflowSpec.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load workflow spec " + classpathResource, e);
        }
    }
}
