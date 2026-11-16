package com.example.correlation.controller;

import com.example.correlation.dto.DashboardSummaryResponse;
import com.example.correlation.service.DashboardMetricsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {
    private final DashboardMetricsService dashboardMetricsService;

    public DashboardController(DashboardMetricsService dashboardMetricsService) {
        this.dashboardMetricsService = dashboardMetricsService;
    }

    @GetMapping("/summary")
    public ResponseEntity<DashboardSummaryResponse> summary(
            @RequestParam(value = "windowHours", defaultValue = "24") int windowHours) {
        return ResponseEntity.ok(dashboardMetricsService.summarize(windowHours));
    }
}
