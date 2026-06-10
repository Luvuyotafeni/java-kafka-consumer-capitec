package com.payments.consumer.repository;

import com.payments.consumer.model.ProcessedPayment;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple in-memory store for processed payments.
 *
 * In a production consumer this would be a database repository.
 * ConcurrentHashMap is used because the Kafka listener thread and
 * the HTTP request thread both access this store concurrently.
 */
@Repository
public class ProcessedPaymentRepository {

    private final Map<String, ProcessedPayment> store = new ConcurrentHashMap<>();

    public void save(ProcessedPayment payment) {
        store.put(payment.eventId(), payment);
    }

    public Optional<ProcessedPayment> findById(String eventId) {
        return Optional.ofNullable(store.get(eventId));
    }

    public List<ProcessedPayment> findAll() {
        return new ArrayList<>(store.values());
    }

    public int count() {
        return store.size();
    }

    public boolean exists(String eventId) {
        return store.containsKey(eventId);
    }
}
