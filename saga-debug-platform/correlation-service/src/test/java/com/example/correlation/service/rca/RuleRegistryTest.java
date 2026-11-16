package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.rca.CandidateStatus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RuleRegistryTest {
    private static RuleRegistry loaded() {
        RuleRegistry registry = new RuleRegistry();
        registry.load();
        return registry;
    }

    @Test
    void loadsTheNineDeclaredRules() {
        assertThat(loaded().ruleIds())
                .containsExactlyInAnyOrder("R03", "R04", "R07", "R09", "R12", "R13", "R15", "R19", "R22");
    }

    @Test
    void everyLoadedRuleIsWellFormed() {
        assertThat(loaded().rules())
                .allSatisfy(rule -> assertThat(rule.validationError()).isEmpty());
    }

    @Test
    void declaredRulesKeepTheirSeverityAndTier() {
        assertThat(rule("R03").getSeverity()).isEqualTo("CRITICAL");
        assertThat(rule("R04").getSeverity()).isEqualTo("HIGH");
        assertThat(rule("R12").getSeverity()).isEqualTo("MEDIUM");
        assertThat(rule("R13").getSeverity()).isEqualTo("LOW");

        assertThat(loaded().rules())
                .allSatisfy(r -> assertThat(r.getEvidence()).isEqualTo(CandidateStatus.STRONG_EVIDENCE));
    }

    @Test
    void everyRuleCarriesSomethingActionable() {
        assertThat(loaded().rules()).allSatisfy(r -> assertThat(r.getSuggestions()).isNotEmpty());
    }

    @Test
    void firesOnItsDeclaredAnomalyType() {
        List<Anomaly> match = rule("R07").matching(List.of(anomaly(1L, AnomalyType.STEP_TIMEOUT)));

        assertThat(match).extracting(Anomaly::getAnomalyType).containsExactly(AnomalyType.STEP_TIMEOUT);
    }

    @Test
    void staysSilentOnAnUnrelatedAnomaly() {
        assertThat(rule("R07").matching(List.of(anomaly(1L, AnomalyType.ORPHAN_EVENT)))).isEmpty();
    }

    @Test
    void staysSilentWhenThereAreNoAnomalies() {
        assertThat(rule("R07").matching(List.of())).isEmpty();
        assertThat(rule("R07").matching(null)).isEmpty();
    }

    @Test
    void eitherAlternativeSignatureFiresTheRule() {
        assertThat(rule("R04").matching(List.of(anomaly(1L, AnomalyType.FORBIDDEN_POST_TERMINAL)))).isNotEmpty();
        assertThat(rule("R04").matching(List.of(anomaly(2L, AnomalyType.STATE_REGRESSION)))).isNotEmpty();

        assertThat(rule("R13").matching(List.of(anomaly(3L, AnomalyType.EXACT_DUPLICATE_EVENT)))).isNotEmpty();
        assertThat(rule("R13").matching(List.of(anomaly(4L, AnomalyType.DUPLICATE_COMPENSATION)))).isNotEmpty();
    }

    @Test
    void quotesEveryMatchEarliestFirstWhateverOrderTheyArriveIn() {
        Anomaly later = anomaly(2L, AnomalyType.EXACT_DUPLICATE_EVENT);
        later.setMeasuredAt(java.time.Instant.parse("2026-01-01T00:00:05Z"));
        Anomaly earlier = anomaly(3L, AnomalyType.DUPLICATE_COMPENSATION);
        earlier.setMeasuredAt(java.time.Instant.parse("2026-01-01T00:00:01Z"));
        Anomaly unrelated = anomaly(1L, AnomalyType.ORPHAN_EVENT);

        assertThat(rule("R13").matching(List.of(unrelated, later, earlier)))
                .extracting(Anomaly::getId).containsExactly(3L, 2L);
        assertThat(rule("R13").matching(List.of(earlier, unrelated, later)))
                .extracting(Anomaly::getId).containsExactly(3L, 2L);
    }

    @Test
    void toleratesANullEntryAmongTheAnomalies() {
        List<Anomaly> withNull = java.util.Arrays.asList(null, anomaly(9L, AnomalyType.STEP_TIMEOUT));

        assertThat(rule("R07").matching(withNull))
                .extracting(Anomaly::getId).containsExactly(9L);
    }

    @Test
    void aRuleWithNoTriggerIsRejectedRatherThanLoadedAndSilent() {
        AnomalySignatureRule empty = new AnomalySignatureRule();
        empty.setId("RXX");
        empty.setRootCause("SOMETHING");
        empty.setSeverity("LOW");

        assertThat(empty.validationError()).hasValueSatisfying(
                reason -> assertThat(reason).contains("can never fire"));
    }

    @Test
    void aRuleWithNoRootCauseIsRejected() {
        AnomalySignatureRule noCause = new AnomalySignatureRule();
        noCause.setId("RXX");
        noCause.setSeverity("LOW");
        noCause.setMatchesAnyAnomaly(List.of(AnomalyType.SAGA_STUCK));

        assertThat(noCause.validationError()).hasValue("no rootCause");
    }

    @Test
    void aRuleWithNoIdIsRejected() {
        AnomalySignatureRule noId = new AnomalySignatureRule();
        noId.setRootCause("SOMETHING");
        noId.setSeverity("LOW");
        noId.setMatchesAnyAnomaly(List.of(AnomalyType.SAGA_STUCK));

        assertThat(noId.validationError()).hasValue("no id");
    }

    @Test
    void repeatedAnomalyTypesInOneRuleAreCollapsed() {
        AnomalySignatureRule rule = new AnomalySignatureRule();
        rule.setMatchesAnyAnomaly(List.of(
                AnomalyType.SAGA_STUCK, AnomalyType.SAGA_STUCK, AnomalyType.STEP_TIMEOUT));

        assertThat(rule.getMatchesAnyAnomaly())
                .containsExactly(AnomalyType.SAGA_STUCK, AnomalyType.STEP_TIMEOUT);
    }

    @Test
    void proceduralRulesAreNotDeclaredHere() {
        assertThat(loaded().ruleIds())
                .doesNotContain("R01", "R02", "R05", "R06", "R10", "R11", "R14", "R20", "R21");
    }

    private static AnomalySignatureRule rule(String id) {
        return loaded().rules().stream()
                .filter(r -> id.equals(r.getId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("rule " + id + " is not declared"));
    }

    private static Anomaly anomaly(Long id, AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setSagaId("saga-1");
        anomaly.setAnomalyType(type);
        anomaly.setDescription(type.name() + " detected");
        return anomaly;
    }
}
