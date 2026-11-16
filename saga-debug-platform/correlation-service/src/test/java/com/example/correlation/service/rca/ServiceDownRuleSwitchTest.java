package com.example.correlation.service.rca;

import com.example.correlation.repository.ServiceLifecycleEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ServiceDownRuleSwitchTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()

            .withInitializer(ctx -> ctx.getBeanFactory().setConversionService(
                    org.springframework.boot.convert.ApplicationConversionService.getSharedInstance()))
            .withBean(ServiceLifecycleEventRepository.class, () -> mock(ServiceLifecycleEventRepository.class))
            .withUserConfiguration(ServiceDownRule.class);

    @Test
    void onByDefault() {
        context.run(c -> assertThat(c).hasSingleBean(ServiceDownRule.class));
    }

    @Test
    void offMeansTheRuleIsNotThere() {
        context.withPropertyValues("correlation.liveness.enabled=false")
                .run(c -> assertThat(c).doesNotHaveBean(ServiceDownRule.class));
    }
}
