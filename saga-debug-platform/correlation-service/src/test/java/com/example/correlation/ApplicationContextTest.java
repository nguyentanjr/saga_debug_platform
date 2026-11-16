package com.example.correlation;

import com.example.correlation.messaging.SagaEventConsumer;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.RetentionService;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@SpringBootTest
@ActiveProfiles("test")
class ApplicationContextTest {
    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoadsWithEveryRepositoryQueryDerived() {
        assertThat(context.getBean(SagaInstanceRepository.class)).isNotNull();
        assertThat(context.getBean(SagaEventRepository.class)).isNotNull();
        assertThat(context.getBean(AnomalyRepository.class)).isNotNull();
        assertThat(context.getBean(RcaResultRepository.class)).isNotNull();
    }

    @Test
    void coreServicesAreWired() {
        assertThat(context.getBean(CorrelationService.class)).isNotNull();
        assertThat(context.getBean(AnomalyDetectionService.class)).isNotNull();
        assertThat(context.getBean(SagaEventConsumer.class)).isNotNull();
    }

    @Test
    void rcaServiceResolvesItsOwnLazyProxy() {
        assertThat(context.getBean(RcaService.class)).isNotNull();
    }

    @Test
    void retentionIsAbsentUnlessExplicitlyEnabled() {
        assertThat(context.getBeanNamesForType(RetentionService.class)).isEmpty();
    }

    @Test
    void everyWorkflowOnTheClasspathIsRegistered() {
        WorkflowRegistry registry = context.getBean(WorkflowRegistry.class);

        assertThat(registry.workflowNames())
                .contains("ORDER_SAGA", "FULFILLMENT_SAGA");
        assertThat(registry.forStartEvent("ORDER_CREATED"))
                .get().extracting(WorkflowSpec::getName).isEqualTo("ORDER_SAGA");
        assertThat(registry.forStartEvent("SHIPMENT_CREATED"))
                .get().extracting(WorkflowSpec::getName).isEqualTo("FULFILLMENT_SAGA");
        assertThat(registry.forStartEvent("PAYMENT_COMPLETED")).isEmpty();
    }

    @Test
    void everyScheduledPassRunsAgainstTheTestSchema() {
        AnomalyDetectionService detection = context.getBean(AnomalyDetectionService.class);
        RcaService rca = context.getBean(RcaService.class);

        assertThatCode(() -> {
            detection.detectStuckSagas();
            detection.detectStepTimeouts();
            detection.detectMissingTerminalEvents();
            detection.detectCompensationMissing();
            detection.detectMissingCompensationAck();
            detection.detectDelayedCompensation();
            rca.autoAnalyzeNewlyDetectedAnomalies();
            rca.autoAnalyzeEndedRuns();
        }).doesNotThrowAnyException();
    }

    @Test
    void kafkaErrorHandlerIsRegistered() {
        assertThat(context.getBean(DefaultErrorHandler.class)).isNotNull();
    }
}
