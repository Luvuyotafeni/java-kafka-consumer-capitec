package com.payments.consumer.controller;

import com.payments.consumer.model.ProcessedPayment;
import com.payments.consumer.repository.ProcessedPaymentRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Read-only REST API exposing consumed payments.
 * Useful for verifying that messages are being received and processed correctly.
 *
 * GET /payments              — all processed payments
 * GET /payments/{eventId}    — single payment by eventId
 * GET /payments/stats        — count and summary
 */
@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final ProcessedPaymentRepository repository;

    public PaymentController(ProcessedPaymentRepository repository) {
        this.repository = repository;
    }

    /** Returns all payments processed so far (most recent first). */
    @GetMapping
    public ResponseEntity<List<ProcessedPayment>> getAll() {
        var payments = repository.findAll()
                .stream()
                .sorted((a, b) -> b.processedAt().compareTo(a.processedAt()))
                .toList();
        return ResponseEntity.ok(payments);
    }

    /** Returns a single payment by its eventId. */
    @GetMapping("/{eventId}")
    public ResponseEntity<ProcessedPayment> getById(@PathVariable String eventId) {
        return repository.findById(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /** Returns a quick summary — useful for smoke testing. */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        var all = repository.findAll();
        long processed = all.stream().filter(p -> "PROCESSED".equals(p.status())).count();
        long failed    = all.stream().filter(p -> "FAILED".equals(p.status())).count();

        return ResponseEntity.ok(Map.of(
                "total",     repository.count(),
                "processed", processed,
                "failed",    failed
        ));
    }
}
