package com.example.correlation.scheduling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;

import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.RetentionService;

class ScheduledSweepsTest {
    private SweepLease lease;
    private AnomalyDetectionService detection;
    private RcaService rca;
    private RetentionService retention;
    private ObjectProvider<RetentionService> retentionProvider;
    private ScheduledSweeps sweeps;

    @BeforeEach
    void setUp() {
        lease = mock(SweepLease.class);
        detection = mock(AnomalyDetectionService.class);
        rca = mock(RcaService.class);
        retention = mock(RetentionService.class);
        retentionProvider = providerOf(retention);
        sweeps = new ScheduledSweeps(lease, detection, rca, retentionProvider);
    }

    private void grantEveryLease() {
        when(lease.runExclusively(anyString(), any(Duration.class), any(Runnable.class)))
                .thenAnswer(call -> {
                    call.getArgument(2, Runnable.class).run();
                    return true;
                });
    }

    @Test
    @DisplayName("every @Scheduled method on this class takes a lease before doing anything")
    void noSweepRunsWithoutALease() throws Exception {
        grantEveryLease();
        List<Method> scheduled = scheduledMethods();
        assertThat(scheduled).hasSize(9);

        for (Method method : scheduled) {
            method.invoke(sweeps);
        }

        ArgumentCaptor<String> names = ArgumentCaptor.forClass(String.class);
        verify(lease, org.mockito.Mockito.times(scheduled.size()))
                .runExclusively(names.capture(), any(Duration.class), any(Runnable.class));
        assertThat(names.getAllValues()).doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("a refused lease means the sweep does no work here at all")
    void refusedLeaseRunsNothing() throws Exception {
        when(lease.runExclusively(anyString(), any(Duration.class), any(Runnable.class)))
                .thenReturn(false);

        for (Method method : scheduledMethods()) {
            method.invoke(sweeps);
        }

        verifyNoInteractions(detection, rca, retention);
    }

    @Test
    @DisplayName("each sweep reaches the service method it is named for")
    void eachSweepDelegatesToItsService() {
        grantEveryLease();

        sweeps.detectStuckSagas();
        sweeps.detectStepTimeouts();
        sweeps.detectMissingTerminalEvents();
        sweeps.detectCompensationMissing();
        sweeps.detectMissingCompensationAck();
        sweeps.detectDelayedCompensation();
        sweeps.autoAnalyzeNewlyDetectedAnomalies();
        sweeps.autoAnalyzeEndedRuns();
        sweeps.purgeExpiredSagas();

        verify(detection).detectStuckSagas();
        verify(detection).detectStepTimeouts();
        verify(detection).detectMissingTerminalEvents();
        verify(detection).detectCompensationMissing();
        verify(detection).detectMissingCompensationAck();
        verify(detection).detectDelayedCompensation();
        verify(rca).autoAnalyzeNewlyDetectedAnomalies();
        verify(rca).autoAnalyzeEndedRuns();
        verify(retention).purgeExpiredSagas();
    }

    @Test
    @DisplayName("the detectors themselves carry no schedule, so a test can call one directly")
    void theScheduleLivesOnlyHere() {
        assertThat(annotatedMethodsOf(AnomalyDetectionService.class)).isEmpty();
        assertThat(annotatedMethodsOf(RcaService.class)).isEmpty();
        assertThat(annotatedMethodsOf(RetentionService.class)).isEmpty();
        assertThat(annotatedMethodsOf(ScheduledSweeps.class)).hasSize(9);
    }

    @Test
    @DisplayName("a lease is never shorter than the sweep it guards is expected to take")
    void leasesAreGenerous() {
        grantEveryLease();
        sweeps.detectStuckSagas();
        sweeps.autoAnalyzeNewlyDetectedAnomalies();
        sweeps.purgeExpiredSagas();

        ArgumentCaptor<Duration> leases = ArgumentCaptor.forClass(Duration.class);
        verify(lease, org.mockito.Mockito.times(3))
                .runExclusively(anyString(), leases.capture(), any(Runnable.class));

        assertThat(leases.getAllValues())
                .allSatisfy(d -> assertThat(d).isGreaterThanOrEqualTo(Duration.ofMinutes(2)));
    }

    @Test
    @DisplayName("a sweep that throws is not swallowed on the way out")
    void failuresPropagate() {
        grantEveryLease();
        org.mockito.Mockito.doThrow(new IllegalStateException("detector blew up"))
                .when(detection).detectStuckSagas();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sweeps.detectStuckSagas())
                .isInstanceOf(IllegalStateException.class);
        verify(rca, never()).autoAnalyzeNewlyDetectedAnomalies();
    }

    @Test
    @DisplayName("with retention switched off the purge takes no lease and does nothing")
    void absentRetentionIsNotEvenAContender() {
        grantEveryLease();
        ScheduledSweeps withoutRetention =
                new ScheduledSweeps(lease, detection, rca, providerOf(null));

        withoutRetention.purgeExpiredSagas();

        verify(lease, never()).runExclusively(anyString(), any(Duration.class), any(Runnable.class));
        verifyNoInteractions(retention);
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<RetentionService> providerOf(RetentionService bean) {
        ObjectProvider<RetentionService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(bean);
        return provider;
    }

    private List<Method> scheduledMethods() {
        return annotatedMethodsOf(ScheduledSweeps.class);
    }

    private static List<Method> annotatedMethodsOf(Class<?> type) {
        List<Method> found = new ArrayList<>();
        Arrays.stream(type.getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Scheduled.class))
                .forEach(found::add);
        return found;
    }
}
