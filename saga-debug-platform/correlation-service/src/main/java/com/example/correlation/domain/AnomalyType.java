package com.example.correlation.domain;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum AnomalyType {
    SAGA_STUCK(Claim.ABSENCE),
    STEP_TIMEOUT(Claim.OCCURRENCE),
    MISSING_TERMINAL_EVENT(Claim.ABSENCE),

    OUT_OF_ORDER(Claim.OCCURRENCE),
    STATE_REGRESSION(Claim.OCCURRENCE),
    FORBIDDEN_POST_TERMINAL(Claim.OCCURRENCE),

    INDETERMINATE_ORDER(Claim.ABSENCE),

    EXACT_DUPLICATE_EVENT(Claim.OCCURRENCE),
    DUPLICATE_COMPENSATION(Claim.OCCURRENCE),

    INVALID_TRANSITION(Claim.OCCURRENCE),
    MULTI_TERMINAL_CONFLICT(Claim.OCCURRENCE),

    COMPENSATION_MISSING(Claim.ABSENCE),
    MISSING_COMPENSATION_ACK(Claim.ABSENCE),

    ORPHAN_EVENT(Claim.ABSENCE),

    MISSING_TRACE_CONTEXT(Claim.ABSENCE),

    RETRY_BURST(Claim.OCCURRENCE),

    DELAYED_COMPENSATION(Claim.OCCURRENCE);

    public enum Claim {
        OCCURRENCE,

        ABSENCE
    }

    private final Claim claim;

    AnomalyType(Claim claim) {
        this.claim = claim;
    }

    public Claim claim() {
        return claim;
    }

    public boolean assertsAbsence() {
        return claim == Claim.ABSENCE;
    }

    public static Set<AnomalyType> absenceClaims() {
        return Arrays.stream(values())
                .filter(AnomalyType::assertsAbsence)
                .collect(Collectors.toUnmodifiableSet());
    }
}
