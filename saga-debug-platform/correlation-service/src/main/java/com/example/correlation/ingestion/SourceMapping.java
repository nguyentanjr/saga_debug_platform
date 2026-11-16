package com.example.correlation.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceMapping {
    private static final ObjectMapper JSON = new ObjectMapper();

    private String name;
    private String topicPattern;

    private String cluster;
    private Fields fields = new Fields();

    private Map<String, String> eventTypes = new LinkedHashMap<>();

    private String system;

    private String aliasSource;

    private String workflow;

    private Pattern compiledPattern;

    private List<String> redact = new ArrayList<>();

    public static final String REDACTED = "[redacted]";

    public List<String> getRedact() { return redact; }
    public void setRedact(List<String> redact) { this.redact = redact != null ? redact : new ArrayList<>(); }

    public String redact(String rawPayload) {
        if (redact.isEmpty() || rawPayload == null) return rawPayload;
        JsonNode tree;
        try {
            tree = JSON.readTree(rawPayload);
        } catch (Exception e) {
            return rawPayload;
        }
        if (tree == null || !tree.isObject()) return rawPayload;
        boolean changed = false;
        for (String path : redact) {
            String[] segments = path.split("\\.");
            JsonNode parent = tree;
            for (int i = 0; i < segments.length - 1 && parent != null; i++) {
                parent = parent.get(segments[i]);
            }
            String leaf = segments[segments.length - 1];
            if (parent instanceof com.fasterxml.jackson.databind.node.ObjectNode object && object.has(leaf)) {
                object.put(leaf, REDACTED);
                changed = true;
            }
        }
        return changed ? tree.toString() : rawPayload;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getTopicPattern() { return topicPattern; }
    public void setTopicPattern(String topicPattern) {
        this.topicPattern = topicPattern;
        this.compiledPattern = topicPattern != null ? Pattern.compile(topicPattern) : null;
    }

    public Fields getFields() { return fields; }
    public void setFields(Fields fields) { this.fields = fields != null ? fields : new Fields(); }

    public Map<String, String> getEventTypes() { return eventTypes; }
    public void setEventTypes(Map<String, String> eventTypes) {
        this.eventTypes = eventTypes != null ? eventTypes : new LinkedHashMap<>();
    }

    public String getWorkflow() { return workflow; }
    public void setWorkflow(String workflow) { this.workflow = workflow; }

    public String getAliasSource() { return aliasSource; }
    public void setAliasSource(String aliasSource) { this.aliasSource = aliasSource; }

    public String getSystem() { return system; }
    public void setSystem(String system) { this.system = system; }

    public String systemName() {
        return system != null && !system.isBlank() ? system : name;
    }

    public String aliasNamespace() {
        return aliasSource != null && !aliasSource.isBlank() ? aliasSource : systemName();
    }

    public String getCluster() { return cluster; }
    public void setCluster(String cluster) { this.cluster = cluster; }

    public boolean matches(String topic) {
        return compiledPattern != null && topic != null && compiledPattern.matcher(topic).matches();
    }

    public boolean matches(String origin, String topic) {
        if (!matches(topic)) return false;
        if (cluster == null || cluster.isBlank()) return true;
        return cluster.equals(origin);
    }

    public String translateEventType(String sourceType) {
        if (sourceType == null) return null;

        String exact = eventTypes.get(sourceType);
        if (exact != null) {
            return exact;
        }

        for (Map.Entry<String, String> candidate : eventTypes.entrySet()) {
            try {
                if (Pattern.matches(candidate.getKey(), sourceType)) {
                    return candidate.getValue();
                }
            } catch (PatternSyntaxException e) {
            }
        }
        return sourceType;
    }

    public Optional<String> eventId(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getEventId());
    }
    public Optional<String> executionId(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getExecutionId());
    }
    public Optional<String> stepName(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getStepName());
    }
    public Optional<String> serviceName(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getServiceName());
    }
    public Optional<String> eventTime(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getEventTime());
    }
    public Optional<String> causationId(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getCausationId());
    }
    public Optional<String> spanId(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getSpanId());
    }
    public Optional<String> parentSpanId(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getParentSpanId());
    }
    public Optional<String> traceId(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getTraceId());
    }
    public Optional<String> businessKey(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getBusinessKey());
    }
    public Optional<String> aliasKey(JsonNode payload, RawObservation observation) {
        return resolve(payload, observation, fields.getAliasKey());
    }

    public Optional<String> eventType(JsonNode payload, RawObservation observation) {
        Optional<String> declared = resolve(payload, observation, fields.getEventType());
        String sourceType = declared.orElseGet(
                () -> observation != null ? observation.sourceRef() : null);
        return Optional.ofNullable(sourceType).map(this::translateEventType);
    }

    public static final String KEY_SOURCE = "key";
    public static final String SOURCE_REF = "source";
    public static final String HEADER_PREFIX = "header:";
    public static final String CONST_PREFIX = "const:";

    public static final String JOIN_PREFIX = "join:";

    Optional<String> resolve(JsonNode payload, RawObservation observation, List<String> paths) {
        if (paths == null) {
            return Optional.empty();
        }
        for (String path : paths) {
            Optional<String> value = resolve(payload, observation, path);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    Optional<String> resolve(JsonNode payload, RawObservation observation, String path) {
        if (path == null || path.isBlank()) {
            return Optional.empty();
        }
        if (KEY_SOURCE.equals(path)) {
            return blankToEmpty(observation != null ? observation.key() : null);
        }
        if (SOURCE_REF.equals(path)) {
            return blankToEmpty(observation != null ? observation.sourceRef() : null);
        }
        if (path.startsWith(HEADER_PREFIX)) {
            String name = path.substring(HEADER_PREFIX.length());
            return blankToEmpty(observation != null ? observation.header(name) : null);
        }
        if (path.startsWith(CONST_PREFIX)) {
            return blankToEmpty(path.substring(CONST_PREFIX.length()));
        }
        if (path.startsWith(JOIN_PREFIX)) {
            List<String> parts = new ArrayList<>();
            for (String part : path.substring(JOIN_PREFIX.length()).split("\\+")) {
                resolve(payload, observation, part).ifPresent(parts::add);
            }
            return blankToEmpty(String.join(":", parts));
        }
        return read(payload, path);
    }

    private static Optional<String> blankToEmpty(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    static Optional<String> read(JsonNode payload, String path) {
        if (payload == null || path == null || path.isBlank()) {
            return Optional.empty();
        }

        JsonNode cursor = payload;
        for (String segment : path.split("\\.")) {
            if (cursor != null && cursor.isTextual()) {
                cursor = parseEmbedded(cursor.asText());
            }

            if (cursor != null && cursor.isArray() && segment.matches("\\d+")) {
                cursor = cursor.get(Integer.parseInt(segment));
                continue;
            }
            if (cursor == null || !cursor.has(segment)) {
                return Optional.empty();
            }
            cursor = cursor.get(segment);
        }

        if (cursor == null || cursor.isNull() || cursor.isMissingNode()) {
            return Optional.empty();
        }
        String value = cursor.isValueNode() ? cursor.asText() : cursor.toString();
        return value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    private static JsonNode parseEmbedded(String text) {
        String trimmed = text == null ? "" : text.trim();
        if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
            return null;
        }
        try {
            return JSON.readTree(trimmed);
        } catch (Exception e) {
            return null;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Fields {
        private List<String> eventId = List.of("eventId");

        private List<String> executionId = List.of("sagaId");
        private List<String> eventType = List.of("eventType");
        private List<String> eventTime = List.of("timestamp");
        private List<String> stepName = List.of("stepName");
        private List<String> serviceName = List.of("serviceName");
        private List<String> causationId = List.of("causationId");
        private List<String> spanId = List.of("spanId");
        private List<String> parentSpanId = List.of("parentSpanId");
        private List<String> traceId = List.of("traceId");

        private List<String> businessKey = List.of("orderId");

        private List<String> aliasKey = List.of();

        public List<String> getEventId() { return eventId; }
        public void setEventId(JsonNode value) { this.eventId = paths(value); }

        public List<String> getExecutionId() { return executionId; }
        public void setExecutionId(JsonNode value) { this.executionId = paths(value); }

        public List<String> getEventType() { return eventType; }
        public void setEventType(JsonNode value) { this.eventType = paths(value); }

        public List<String> getEventTime() { return eventTime; }
        public void setEventTime(JsonNode value) { this.eventTime = paths(value); }

        public List<String> getStepName() { return stepName; }
        public void setStepName(JsonNode value) { this.stepName = paths(value); }

        public List<String> getServiceName() { return serviceName; }
        public void setServiceName(JsonNode value) { this.serviceName = paths(value); }

        public List<String> getCausationId() { return causationId; }
        public void setCausationId(JsonNode value) { this.causationId = paths(value); }

        public List<String> getSpanId() { return spanId; }
        public void setSpanId(JsonNode value) { this.spanId = paths(value); }

        public List<String> getParentSpanId() { return parentSpanId; }
        public void setParentSpanId(JsonNode value) { this.parentSpanId = paths(value); }

        public List<String> getTraceId() { return traceId; }
        public void setTraceId(JsonNode value) { this.traceId = paths(value); }

        public List<String> getBusinessKey() { return businessKey; }
        public void setBusinessKey(JsonNode value) { this.businessKey = paths(value); }

        public List<String> getAliasKey() { return aliasKey; }
        public void setAliasKey(JsonNode value) { this.aliasKey = paths(value); }

        private static List<String> paths(JsonNode value) {
            if (value == null || value.isNull()) {
                return List.of();
            }
            if (value.isArray()) {
                List<String> paths = new ArrayList<>(value.size());
                value.forEach(element -> {
                    String path = element.asText();
                    if (path != null && !path.isBlank()) paths.add(path);
                });
                return List.copyOf(paths);
            }
            String path = value.asText();
            return path == null || path.isBlank() ? List.of() : List.of(path);
        }
    }

    public static String describe(List<String> paths) {
        return paths == null || paths.isEmpty() ? "(undeclared)" : String.join("' or '", paths);
    }

    public static SourceMapping identity() {
        SourceMapping mapping = new SourceMapping();
        mapping.setName("default");
        return mapping;
    }
}
