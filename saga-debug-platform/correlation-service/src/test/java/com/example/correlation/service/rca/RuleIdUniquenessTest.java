package com.example.correlation.service.rca;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class RuleIdUniquenessTest {
    private static final Pattern CODE_RULE =
            Pattern.compile("candidate\\(\\s*\"([A-Z_]+)\"\\s*,\\s*[^,]+,\\s*\"(R\\d+)\"");
    private static final Pattern YAML_RULE =
            Pattern.compile("-\\s*id:\\s*(R\\d+)\\s*\\n\\s*rootCause:\\s*([A-Z_]+)");

    @Test
    void everyRuleIdNamesExactlyOneRootCause() throws Exception {
        Map<String, Set<String>> causesById = new HashMap<>();

        String code = Files.readString(Path.of("src/main/java/com/example/correlation/service/RcaService.java"));
        Matcher m = CODE_RULE.matcher(code);
        while (m.find()) causesById.computeIfAbsent(m.group(2), k -> new HashSet<>()).add(m.group(1));

        String yaml = Files.readString(Path.of("src/main/resources/rules/anomaly-signature-rules.yaml"));
        m = YAML_RULE.matcher(yaml);
        while (m.find()) causesById.computeIfAbsent(m.group(1), k -> new HashSet<>()).add(m.group(2));

        causesById.computeIfAbsent(ServiceDownRule.RULE_ID, k -> new HashSet<>()).add(ServiceDownRule.ROOT_CAUSE);

        assertThat(causesById).as("rule ids found").containsKeys("R01", "R07", "R23", ServiceDownRule.RULE_ID);
        assertThat(causesById).allSatisfy((id, causes) ->
                assertThat(causes).as("root causes claimed by " + id).hasSize(1));
    }
}
