package com.example.correlation.service.alert;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Component
public class FileAlertSink implements AlertSink {
    private static final Logger log = LoggerFactory.getLogger(FileAlertSink.class);

    private final ObjectMapper objectMapper;
    private final Path alertFile;

    public FileAlertSink(ObjectMapper objectMapper,
            @Value("${rca.alert.file:/app/results/alerts-realtime.log}") String alertFilePath) {
        this.objectMapper = objectMapper;
        this.alertFile = Paths.get(alertFilePath);
        try {
            Path parent = alertFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            log.warn("Cannot create alerts directory: {}", e.getMessage());
        }
    }

    @Override
    public synchronized void emit(AlertEvent alert) {
        try {
            String line = objectMapper.writeValueAsString(alert) + System.lineSeparator();
            Files.writeString(alertFile, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to write alert: {}", e.getMessage());
        }
    }
}
