package com.example.correlation.controller;

import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LifecycleEventControllerTest {
    private static final String TOKEN = "t";
    private final ObjectMapper json = new ObjectMapper();
    private final ServiceLifecycleEventRepository repository = mock(ServiceLifecycleEventRepository.class);
    private final LifecycleEventController controller =
            new LifecycleEventController(repository, new IngestAuthorization(TOKEN));

    @Test
    void aProcessExitReportedBySystemdIsRecordedAsTheRuntimeSawIt() throws Exception {
        var response = controller.record(body("""
                {"service":"inventory-service","state":"down","at":"2026-10-04T10:00:05Z",
                 "basis":"process","detail":"exit code 137 (OOM)","source":"systemd","scope":"billing-prod"}"""), "Bearer " + TOKEN);

        assertThat(response.getStatusCode().value()).isEqualTo(201);
        ArgumentCaptor<ServiceLifecycleEvent> saved = ArgumentCaptor.forClass(ServiceLifecycleEvent.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().getServiceName()).isEqualTo("inventory-service");
        assertThat(saved.getValue().getState()).isEqualTo(ServiceLifecycleEvent.State.DOWN);
        assertThat(saved.getValue().getBasis()).isEqualTo(ServiceLifecycleEvent.Basis.PROCESS);
        assertThat(saved.getValue().getOccurredAt()).isEqualTo(Instant.parse("2026-10-04T10:00:05Z"));
        assertThat(saved.getValue().getSource()).isEqualTo("systemd");
        assertThat(saved.getValue().getScope()).isEqualTo("billing-prod");
    }

    @Test
    void withoutABasisNothingIsRecorded() throws Exception {
        var response = controller.record(body("""
                {"service":"inventory-service","state":"DOWN","at":"2026-10-04T10:00:05Z"}"""), "Bearer " + TOKEN);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").toString()).contains("basis");
        verify(repository, never()).save(any());
    }

    @Test
    void malformedReportsAreRejected() throws Exception {
        assertThat(controller.record(body("""
                {"service":"x","state":"PAUSED","at":"2026-10-04T10:00:05Z","basis":"probe"}"""),
                "Bearer " + TOKEN).getStatusCode().value()).isEqualTo(400);
        assertThat(controller.record(body("""
                {"service":"x","state":"DOWN","at":"yesterday","basis":"probe"}"""),
                "Bearer " + TOKEN).getStatusCode().value()).isEqualTo(400);
        assertThat(controller.record(body("""
                {"state":"DOWN","at":"2026-10-04T10:00:05Z","basis":"probe"}"""),
                "Bearer " + TOKEN).getStatusCode().value()).isEqualTo(400);
        verify(repository, never()).save(any());
    }

    @Test
    void writingEvidenceNeedsTheIngestToken() throws Exception {
        var response = controller.record(body("""
                {"service":"x","state":"DOWN","at":"2026-10-04T10:00:05Z","basis":"process"}"""), null);

        assertThat(response.getStatusCode().is4xxClientError()).isTrue();
        verify(repository, never()).save(any());
    }

    @Test
    void aRepeatedReportIsADuplicateNotASecondFact() throws Exception {
        when(repository.existsBySourceAndExternalId(anyString(), anyString())).thenReturn(true);

        var response = controller.record(body("""
                {"service":"x","state":"DOWN","at":"2026-10-04T10:00:05Z","basis":"probe","id":"alert-42"}"""),
                "Bearer " + TOKEN);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("status", "duplicate");
        verify(repository, never()).save(any());
    }

    private JsonNode body(String text) throws Exception {
        return json.readTree(text);
    }
}
