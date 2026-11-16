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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class RuleRegistry {
    private static final Logger log = LoggerFactory.getLogger(RuleRegistry.class);

    @Value("${correlation.declarations.dir:}")
    private String declarationsDirectory;

    private final List<AnomalySignatureRule> rules = new ArrayList<>();

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            List<Resource> resources = DeclarationFiles.find("rules", declarationsDirectory);

            for (Resource resource : resources) {
                try (InputStream is = resource.getInputStream()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                    Object declared = root.get("rules");
                    if (declared == null) {
                        log.error("Rule file {} has no 'rules' key; ignored", resource.getFilename());
                        continue;
                    }
                    List<AnomalySignatureRule> parsed = yamlMapper.convertValue(
                            declared,
                            yamlMapper.getTypeFactory()
                                    .constructCollectionType(List.class, AnomalySignatureRule.class));
                    register(parsed, resource.getFilename());
                } catch (Exception e) {
                    log.error("Failed to load rule file {}: {}",
                            resource.getFilename(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to scan for rule files: {}", e.getMessage(), e);
        }

        if (rules.isEmpty()) {
            log.warn("No anomaly-signature rules declared. Diagnosis falls back to the "
                    + "procedural rules in RcaService only.");
        } else {
            log.info("Loaded {} anomaly-signature rule(s): {}", rules.size(), ruleIds());
        }
    }

    private void register(List<AnomalySignatureRule> parsed, String filename) {
        if (parsed == null) return;
        Set<String> known = ruleIds();

        for (AnomalySignatureRule rule : parsed) {
            if (rule == null) continue;

            var error = rule.validationError();
            if (error.isPresent()) {
                log.error("Rule in {} ignored: {}", filename, error.get());
                continue;
            }
            if (!known.add(rule.getId())) {
                log.error("Duplicate rule id '{}' in {}; keeping the one already loaded",
                        rule.getId(), filename);
                continue;
            }
            rules.add(rule);
            log.debug("Rule '{}' -> {} ({}), fires on {}",
                    rule.getId(), rule.getRootCause(), rule.getEvidence(),
                    rule.getMatchesAnyAnomaly());
        }
    }

    public List<AnomalySignatureRule> rules() {
        return List.copyOf(rules);
    }

    public Set<String> ruleIds() {
        return rules.stream()
                .map(AnomalySignatureRule::getId)
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    }
}
