package com.example.correlation.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SourceCompletenessCheck {
    private static final Logger log = LoggerFactory.getLogger(SourceCompletenessCheck.class);

    private final Set<String> alreadyReported = ConcurrentHashMap.newKeySet();

    public List<String> gaps(JsonNode payload, SourceMapping mapping, RawObservation raw) {
        if (mapping == null) {
            return List.of();
        }

        SourceMapping.Fields fields = mapping.getFields();
        Map<String, List<String>> declared = new LinkedHashMap<>();
        declared.put("eventTime", fields.getEventTime());
        declared.put("stepName", fields.getStepName());
        declared.put("serviceName", fields.getServiceName());

        List<String> gaps = new ArrayList<>();
        declared.forEach((field, paths) -> {
            if (paths == null || paths.isEmpty()) {
                return;
            }
            if (mapping.resolve(payload, raw, paths).isEmpty()) {
                gaps.add(field + " (declared at '" + SourceMapping.describe(paths) + "')");
            }
        });
        return gaps;
    }

    public void report(List<String> gaps, String sourceName, String sourceRef) {
        for (String gap : gaps) {
            if (alreadyReported.add(sourceName + "|" + gap)) {
                log.warn("Source '{}' ({}) declares {} but this event did not carry it; "
                        + "reported once per source", sourceName, sourceRef, gap);
            }
        }
    }
}
