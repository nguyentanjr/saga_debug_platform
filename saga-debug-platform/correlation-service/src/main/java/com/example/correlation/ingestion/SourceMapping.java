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

/**
 * How to read one external system's events into this platform's envelope.
 *
 * <p>Without this the platform can only ingest events that already use its own field
 * names — {@code sagaId}, {@code eventId}, {@code eventType}, {@code timestamp}. No
 * existing system emits those, so attaching the platform to one meant changing that
 * system's producers. A mapping states where each field lives in <em>their</em> payload
 * instead, so an unmodified system can be observed.
 *
 * <h2>What a declared path may say</h2>
 * <ul>
 *   <li>{@code key} — the transport's record key</li>
 *   <li>{@code header:NAME} — a transport header</li>
 *   <li>{@code source} — the topic, queue or source name the observation arrived on</li>
 *   <li>{@code const:VALUE} — VALUE itself, for what is known about the source rather than
 *       carried by it: an outbox topic is published by one service, and saying which in the
 *       mapping beats recording "unknown" for every event on it</li>
 *   <li>anything else — a dotted path into the payload, which is the default</li>
 * </ul>
 *
 * <p>Any field may declare several of those instead of one, and the first that resolves
 * is used. Systems do not keep a field in the same place across every event they publish
 * — a v1 and a v2 of the same event, an id at the top level in one service and nested in
 * another — and the alternative was one mapping file per shape, or asking the source to
 * normalise what it emits.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceMapping {

    /** Reads payloads that were embedded as a JSON string; see {@link #read}. */
    private static final ObjectMapper JSON = new ObjectMapper();

    private String name;
    private String topicPattern;

    /**
     * Which declared Kafka cluster this mapping speaks for, or null for any of them.
     *
     * <p>Only means something in a deployment reading more than one cluster. A topic name
     * is unique inside a cluster and nowhere else, so once two are being read, "the
     * mapping for topic {@code order-created}" is an ambiguous question: both clusters can
     * have one, carrying different systems' runs. Naming the cluster is how a mapping
     * answers it.
     *
     * <p>Left out, the mapping matches whatever cluster the record came from — which is
     * what every single-cluster deployment means, and why no existing declaration has to
     * change.
     */
    private String cluster;
    private Fields fields = new Fields();

    /**
     * Source event type to the name the workflow specification uses.
     *
     * <p>Optional. Systems rarely name their events the way a specification written
     * about them does — {@code order.placed} against {@code ORDER_CREATED} — and asking
     * either side to rename is asking for a change this platform exists to avoid.
     *
     * <p>A key that matches no type exactly is tried as a regular expression, so a source
     * that versions its event names ({@code order.created.v1}, {@code .v2}, {@code .v3})
     * is one line rather than one line per version.
     */
    private Map<String, String> eventTypes = new LinkedHashMap<>();

    /**
     * The observed system this mapping reads, when that is not the mapping's own name.
     *
     * <p>Optional, and defaulting to {@link #name} so a system read by a single file says
     * nothing and behaves exactly as before.
     *
     * <h2>Why a mapping and a system are not the same thing</h2>
     * A mapping is a way of reading one shape of event; a system is an id space. One
     * system frequently needs several mappings, and not for want of trying to write one:
     * the saga in {@code semotpan/saga-orchestration} publishes on four topics whose
     * payloads all carry a field called {@code status}, and {@code CANCELLED} means "the
     * room booking was released" on one topic and "the payment was reversed" on another.
     * No single mapping can translate one field name two ways, and inventing a shared
     * vocabulary would be this platform asking the observed system to make sense on its
     * terms.
     *
     * <p>The namespace half of {@link com.example.correlation.domain.ExecutionKey} used to be taken from {@code name},
     * so those four mappings were four id spaces and one saga id was filed as up to four
     * half-finished executions — each looking stuck, and the payment fragment raising an
     * ORPHAN_EVENT because its first event starts no known workflow. Declaring
     * {@code system:} on each mapping is how several files say they are reading one
     * system, and it is a statement in a mapping file rather than something the platform
     * infers from two identifiers matching.
     *
     * <p>This does not weaken the protection {@code ExecutionKey} exists for. Two systems
     * that merely share an identifier still have different {@code system} values and so
     * still cannot reach each other; folding them together requires someone to write the
     * same name in both files, which is the same deliberate act as declaring
     * {@link #aliasSource}.
     */
    private String system;

    /**
     * The namespace an {@code aliasKey} from this source belongs to.
     *
     * <p>Null, and normally left so: an alias identifies a run of the same system, and
     * scoping it to this source is what stops two observed systems that happen to share
     * an identifier from resolving to each other.
     *
     * <p>Names a {@code system}, not a mapping. Set it only to state deliberately that
     * this source's alias names a run in <em>another</em> observed system — a gateway that publishes both its own reference
     * and the partner's, say. That is the one path by which two systems can be folded into
     * one execution, so it has to be a sentence someone wrote in a mapping file, backed by
     * the event carrying both, rather than something the platform arrives at on its own.
     */
    private String aliasSource;

    /**
     * The workflow this source's events belong to.
     *
     * <p>Optional. It exists for the deployment that watches several systems whose event
     * names overlap: {@code ORDER_CREATED} is what an order system's first event is called
     * in every company that has one, so the event name alone stops being enough to say
     * which specification a run should be judged against.
     *
     * <p>The same thing can be said from the specification's side with {@code sources:},
     * which is usually the better place because a workflow knows what it describes. This
     * is here for the case where a source has exactly one workflow and its owner would
     * rather say so once in the mapping than edit a specification they did not write.
     */
    private String workflow;

    private Pattern compiledPattern;

    /**
     * Dotted payload paths whose values this platform must not keep (L14).
     *
     * <p>What is sensitive is a fact about the observed system, so it is declared here with
     * the rest of what is known about that system — the source changes nothing. Applied to
     * the payload that is stored, after every field above has been read from the original,
     * so correlation, identity and event time are unaffected. Paths walk JSON objects only;
     * a value inside a payload embedded as a string is not reached.
     */
    private List<String> redact = new ArrayList<>();

    /** What a redacted value is replaced with. */
    public static final String REDACTED = "[redacted]";

    public List<String> getRedact() { return redact; }
    public void setRedact(List<String> redact) { this.redact = redact != null ? redact : new ArrayList<>(); }

    /**
     * The payload with every declared path that is present replaced by {@link #REDACTED}.
     * Returned unchanged, byte for byte, when there is nothing to replace or it is not a
     * JSON object — stored payloads are evidence, and are not re-serialised for nothing.
     */
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

    /**
     * The id space observations read by this mapping belong to.
     *
     * <p>The declared system, or the mapping's own name when none is declared. This is the
     * namespace half of every {@link com.example.correlation.domain.ExecutionKey} this mapping produces, and the value
     * {@code source_name} stores beside it.
     */
    public String systemName() {
        return system != null && !system.isBlank() ? system : name;
    }

    /** The namespace this source's alias keys live in: another system only if declared. */
    public String aliasNamespace() {
        return aliasSource != null && !aliasSource.isBlank() ? aliasSource : systemName();
    }

    public String getCluster() { return cluster; }
    public void setCluster(String cluster) { this.cluster = cluster; }

    public boolean matches(String topic) {
        return compiledPattern != null && topic != null && compiledPattern.matcher(topic).matches();
    }

    /**
     * Whether this mapping reads a topic that arrived on a particular cluster.
     *
     * <p>A mapping that names no cluster reads the topic wherever it came from; a mapping
     * that names one reads it only from there. An observation with no origin -- an HTTP
     * post, or a deployment reading a single cluster -- is only ever matched by the first
     * kind, because a mapping that named a cluster said something the observation cannot
     * confirm, and guessing is what invariant I8 forbids.
     */
    public boolean matches(String origin, String topic) {
        if (!matches(topic)) return false;
        if (cluster == null || cluster.isBlank()) return true;
        return cluster.equals(origin);
    }

    /**
     * The declared name for a source event type, or the type unchanged.
     *
     * <p>Exact match first: a source whose types are literal strings must not have them
     * reinterpreted as patterns. Only a type nothing matches exactly is tried against the
     * keys as regular expressions, in declaration order.
     */
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
                // A key that is neither an exact match nor a valid pattern is simply a
                // type this source does not translate.
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

    /**
     * The event type as the workflow specification names it.
     *
     * <p>Falls back to the source name when no declared path resolves, and translates
     * that too. Plenty of systems publish one event type per topic and put nothing in the
     * body to say which — the topic name <em>is</em> the type. Before the fallback was
     * translated, those systems reached the specification under their own raw topic names,
     * matched no declared step, and the platform reported an execution it could not judge
     * rather than reading what was in front of it.
     */
    public Optional<String> eventType(JsonNode payload, RawObservation observation) {
        Optional<String> declared = resolve(payload, observation, fields.getEventType());
        String sourceType = declared.orElseGet(
                () -> observation != null ? observation.sourceRef() : null);
        return Optional.ofNullable(sourceType).map(this::translateEventType);
    }


    /** Where a declared path says to look. */
    public static final String KEY_SOURCE = "key";
    public static final String SOURCE_REF = "source";
    public static final String HEADER_PREFIX = "header:";
    public static final String CONST_PREFIX = "const:";

    /**
     * Several fields read as one value, {@code join:data.cmd+data.response.status}, parts that
     * are absent left out. For a source whose meaning needs two of them: ewallet's core payment
     * replies SUCCESS_DEDUCT to both its hold and its settlement, and only the command says which.
     */
    public static final String JOIN_PREFIX = "join:";


    /** The first of these paths that resolves to a value. */
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

    /**
     * Reads one declared field from wherever the mapping says it lives.
     *
     * <p>Prefixes rather than a nested {@code {source, path}} object because a mapping is
     * read far more often than it is written, and {@code eventType: header:eventType} says
     * the whole thing on one line. The cost is a known limitation, recorded here rather
     * than hidden: a payload field named exactly {@code key} or {@code source}, or one
     * whose name starts with {@code header:} or {@code const:}, cannot be addressed. None has been seen in
     * practice, and the readability is worth that.
     */
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

    /**
     * Follows a dotted path.
     *
     * <h2>Payloads embedded as a string</h2>
     * A path with further segments to walk that lands on a text node treats that text as
     * an embedded document. This is the ordinary shape of the transactional outbox
     * pattern: the outbox table stores the event body in a {@code text} column, so
     * Debezium publishes it as a JSON <em>string</em> and every path into it stopped at
     * the wall — the source was unreadable however it was mapped, and the only remedy was
     * to ask its owners to change the connector. Unwrapping is not a guess: the mapping
     * asked to descend, so the value was meant to be a document.
     *
     * @return empty when the path is undeclared, absent from this payload, or explicitly
     *         null — all three mean the same thing to a caller: this source does not
     *         provide the field for this event
     */
    static Optional<String> read(JsonNode payload, String path) {
        if (payload == null || path == null || path.isBlank()) {
            return Optional.empty();
        }

        JsonNode cursor = payload;
        for (String segment : path.split("\\.")) {
            if (cursor != null && cursor.isTextual()) {
                cursor = parseEmbedded(cursor.asText());
            }
            // A numeric segment indexes an array: data.response.paymentSources.0.status.
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

    /** @return the document this text holds, or null when it is just text */
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

    /**
     * Where each envelope field lives.
     *
     * <p>Defaults are this platform's own field names read from the payload, so a producer
     * that already speaks the envelope needs no mapping at all and an existing deployment
     * is unaffected.
     *
     * <p>Every field accepts one path or a list of them. Both of these are valid, and the
     * second means "whichever of these this event happens to carry":
     *
     * <pre>
     *   executionId: context.orderNumber
     *   executionId: [key, context.orderNumber, orderId]
     * </pre>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Fields {
        private List<String> eventId = List.of("eventId");
        /** The correlation key: what ties observations into one execution. */
        private List<String> executionId = List.of("sagaId");
        private List<String> eventType = List.of("eventType");
        private List<String> eventTime = List.of("timestamp");
        private List<String> stepName = List.of("stepName");
        private List<String> serviceName = List.of("serviceName");
        private List<String> causationId = List.of("causationId");
        private List<String> spanId = List.of("spanId");
        private List<String> parentSpanId = List.of("parentSpanId");
        private List<String> traceId = List.of("traceId");

        /**
         * The business identifier an operator searches by — an order number, a booking
         * reference. Distinct from {@code executionId}, which only has to be unique.
         */
        private List<String> businessKey = List.of("orderId");

        /**
         * A second identifier this event carries for the same execution.
         *
         * <p>Undeclared by default. Declared for a system whose services do not share one
         * id; see {@link ExecutionAliasResolver}.
         */
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

        /** A scalar, a sequence, or an explicit null meaning "this source has no such field". */
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

    /** The declared paths for one field, as they read in a message to an operator. */
    public static String describe(List<String> paths) {
        return paths == null || paths.isEmpty() ? "(undeclared)" : String.join("' or '", paths);
    }

    /** The identity mapping: a source already speaking this platform's envelope. */
    public static SourceMapping identity() {
        SourceMapping mapping = new SourceMapping();
        mapping.setName("default");
        return mapping;
    }
}
