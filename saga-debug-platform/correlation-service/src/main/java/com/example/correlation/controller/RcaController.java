package com.example.correlation.controller;

import com.example.correlation.domain.RcaResult;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.service.RcaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/rca")
public class RcaController {
    private final RcaService rcaService;
    private final ExecutionLookup executionLookup;

    public RcaController(RcaService rcaService, ExecutionLookup executionLookup) {
        this.rcaService = rcaService;
        this.executionLookup = executionLookup;
    }

    @PostMapping("/analyze/{sagaId}")
    public ResponseEntity<?> analyze(@PathVariable("sagaId") String sagaId) {
        String executionId = executionLookup.resolve(sagaId).orElse(null);
        if (executionId == null) {
            return ResponseEntity.notFound().build();
        }
        RcaResult result = rcaService.analyze(executionId);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }

    @GetMapping("/results/{sagaId}")
    public ResponseEntity<List<RcaResult>> getResults(@PathVariable("sagaId") String sagaId) {
        String executionId = executionLookup.resolve(sagaId).orElse(null);
        if (executionId == null) {
            return ResponseEntity.ok(List.of());
        }
        List<RcaResult> results = rcaService.getResults(executionId);
        return ResponseEntity.ok(results);
    }
}
