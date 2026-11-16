package com.example.correlation.controller;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

@Component
public class IngestAuthorization {
    private static final Logger log = LoggerFactory.getLogger(IngestAuthorization.class);

    private final byte[] token;

    public IngestAuthorization(@Value("${correlation.ingest.token:}") String token) {
        this.token = token == null || token.isBlank() ? null : token.getBytes(StandardCharsets.UTF_8);
        if (this.token == null) {
            log.warn("correlation.ingest.token is not set; writing observations over HTTP is "
                    + "closed (503). Kafka ingestion is unaffected.");
        }
    }

    public <T> ResponseEntity<T> refusal(String authorizationHeader) {
        if (token == null) {
            return body(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE),
                    "writing observations over HTTP is not configured on this deployment");
        }
        String presented = authorizationHeader != null
                && authorizationHeader.regionMatches(true, 0, "Bearer ", 0, 7)
                ? authorizationHeader.substring(7).trim() : null;

        if (presented == null || !MessageDigest.isEqual(token, presented.getBytes(StandardCharsets.UTF_8))) {
            return body(ResponseEntity.status(HttpStatus.UNAUTHORIZED).header("WWW-Authenticate", "Bearer"),
                    "a valid bearer token is required to submit observations");
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T> ResponseEntity<T> body(ResponseEntity.BodyBuilder builder, String error) {
        return (ResponseEntity<T>) builder.body(Map.of("error", error));
    }
}
