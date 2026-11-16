package com.example.correlation.workflow;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class SourceScopedWorkflowResolutionTest {
    private static final String SHOP = "shop";
    private static final String PARTNER = "partner";

    @Test
    void twoSystemsSharingAnEventNameEachResolveToTheirOwnWorkflow() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        assertThat(resolveFresh(registry, SHOP, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
        assertThat(resolveFresh(registry, PARTNER, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("PARTNER_ORDER_SAGA");
    }

    @Test
    void addingASecondSystemDoesNotUnresolveTheFirst() {
        Optional<WorkflowSpec> alone =
                resolveFresh(registryWith(shopOrderSaga()), SHOP, "ORDER_CREATED");
        Optional<WorkflowSpec> alongside =
                resolveFresh(registryWith(shopOrderSaga(), partnerOrderSaga()), SHOP, "ORDER_CREATED");

        assertThat(alone).map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
        assertThat(alongside).map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
    }

    @Test
    void aSpecificationNamingNoSourceStillAnswersForAnySource() {
        WorkflowRegistry registry = registryWith(unscopedOrderSaga());

        assertThat(resolveFresh(registry, "whatever-source", "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("ORDER_SAGA");
        assertThat(resolveFresh(registry, null, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("ORDER_SAGA");
    }

    @Test
    void anAmbiguityThatNarrowingDoesNotResolveStaysUnresolved() {
        WorkflowRegistry registry = registryWith(unscopedOrderSaga(), unscopedOtherSaga());

        assertThat(resolveFresh(registry, SHOP, "ORDER_CREATED")).isEmpty();
    }

    @Test
    void anUnknownSourceDoesNotBorrowAScopedWorkflow() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        assertThat(resolveFresh(registry, "some-third-system", "ORDER_CREATED")).isEmpty();
    }

    @Test
    void anExecutionThatAlreadyResolvedKeepsItsWorkflow() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        assertThat(registry.resolve("SHOP_ORDER_SAGA", null, PARTNER, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
    }

    @Test
    void theSourceMappingCanNameTheWorkflowWhenSpecificationsDoNot() {
        WorkflowRegistry registry = registryWith(unscopedOrderSaga(), unscopedOtherSaga());

        assertThat(registry.resolve(null, "ORDER_SAGA", SHOP, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("ORDER_SAGA");
    }

    @Test
    void aSpecificationsOwnSourceClaimOutranksTheMappings() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        assertThat(registry.resolve(null, "PARTNER_ORDER_SAGA", SHOP, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
    }

    @Test
    void anUndeclaredStartEventResolvesToNothing() {
        WorkflowRegistry registry = registryWith(shopOrderSaga());

        assertThat(resolveFresh(registry, SHOP, "SOMETHING_ELSE_ENTIRELY")).isEmpty();
    }

    @Test
    void sourceAndMappingDisagreementIsFlaggedOnTheFirstObservation() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());
        var instances = mock(SagaInstanceRepository.class);
        var events = mock(SagaEventRepository.class);
        var detector = mock(AnomalyDetectionService.class);
        var service = new CorrelationService(instances, events, detector,
                new DeduplicationCache(), registry, new ObjectMapper());
        var key = ExecutionKey.of(SHOP, "conflict");
        service.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SHOP, key, "e1", null, CorrelationBasis.DECLARED_KEY),
                "PARTNER_ORDER_SAGA", "ORDER_CREATED", "UNKNOWN", "test", Instant.EPOCH,
                "{}", Instant.EPOCH, IngestedEvent.Causality.NONE));
        var saved = ArgumentCaptor.forClass(SagaInstance.class);
        verify(instances).save(saved.capture());
        assertThat(saved.getValue().getWorkflowType()).isEqualTo("SHOP_ORDER_SAGA");
        assertThat(saved.getValue().getWorkflowConflicts())
                .containsExactly("PARTNER_ORDER_SAGA", "SHOP_ORDER_SAGA");
        verifyNoInteractions(detector);
    }

    private static Optional<WorkflowSpec> resolveFresh(WorkflowRegistry registry,
            String sourceName, String eventType) {
        return registry.resolve(null, null, sourceName, eventType);
    }

    private static WorkflowRegistry registryWith(WorkflowSpec... specs) {
        WorkflowRegistry registry = new WorkflowRegistry();
        for (WorkflowSpec spec : specs) {
            registry.register(spec, spec.getName() + ".yaml");
        }
        return registry;
    }

    private static WorkflowSpec shopOrderSaga() {
        return spec("SHOP_ORDER_SAGA", "sources: [shop]\n");
    }

    private static WorkflowSpec partnerOrderSaga() {
        return spec("PARTNER_ORDER_SAGA", "sources: [partner]\n");
    }

    private static WorkflowSpec unscopedOrderSaga() {
        return spec("ORDER_SAGA", "");
    }

    private static WorkflowSpec unscopedOtherSaga() {
        return spec("OTHER_SAGA", "");
    }

    private static WorkflowSpec spec(String name, String sourcesLine) {
        String yaml = """
                name: %s
                version: "1.0"
                %ssteps:
                  - id: CREATE
                    service: a-service
                    timeout: 30s
                    events:
                      completed: ORDER_CREATED
                  - id: FINISH
                    service: b-service
                    timeout: 30s
                    events:
                      completed: RUN_FINISHED
                      failed: RUN_FAILED
                transitions:
                  - from: STARTED
                    to: CREATE
                    condition: on_success
                  - from: CREATE
                    to: FINISH
                    condition: on_success
                  - from: FINISH
                    to: COMPLETED
                    condition: on_success
                  - from: FINISH
                    to: FAILED
                    condition: on_failure
                """.formatted(name, sourcesLine);
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, WorkflowSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test specification", e);
        }
    }

    @Test
    void theFixturesActuallyCollide() {
        assertThat(shopOrderSaga().startEventTypes()).contains("ORDER_CREATED");
        assertThat(partnerOrderSaga().startEventTypes()).contains("ORDER_CREATED");
        assertThat(Map.of("shop", shopOrderSaga().getSources(),
                        "partner", partnerOrderSaga().getSources()))
                .containsEntry("shop", java.util.List.of("shop"))
                .containsEntry("partner", java.util.List.of("partner"));
    }
}
