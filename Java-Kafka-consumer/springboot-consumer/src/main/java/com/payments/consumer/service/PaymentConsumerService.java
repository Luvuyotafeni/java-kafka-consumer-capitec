package com.payments.consumer.service;

import com.payments.consumer.model.PaymentInitiatedEvent;
import com.payments.consumer.model.ProcessedPayment;
import com.payments.consumer.repository.ProcessedPaymentRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Kafka listener for the payment-initiated topic.
 *
 * Key design decisions:
 *
 * 1. Manual offset commit (Acknowledgment) — the offset is committed only
 *    after business processing succeeds. A crash before acknowledge() means
 *    the message is redelivered, not lost.
 *
 * 2. Consumer idempotency — we check if the eventId was already processed
 *    before doing any work. Kafka guarantees at-least-once delivery even with
 *    idempotent producers; the consumer side must guard against duplicates too.
 *
 * 3. Error handling split:
 *    - Transient errors (DB unavailable, downstream timeout): let the exception
 *      propagate WITHOUT calling acknowledge(). The offset is not advanced;
 *      the message will be redelivered on the next poll.
 *    - Poison-pill / unrecoverable errors (bad data, deserialization failure):
 *      route to DLT and acknowledge so the partition keeps moving.
 *
 * 4. DLT routing — failed records land on payment-initiated.dlt with
 *    diagnostic headers so they can be inspected and replayed.
 */
@Service
public class PaymentConsumerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumerService.class);

    private final ProcessedPaymentRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String dltTopic;

    public PaymentConsumerService(
            ProcessedPaymentRepository repository,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${topic.payment-initiated-dlt}") String dltTopic
    ) {
        this.repository    = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.dltTopic      = dltTopic;
    }

    /**
     * Main listener.
     *
     * groupId and topics are externalised to application.yml so they can be
     * changed per environment without a code change.
     *
     * The ConsumerRecord wrapper gives us access to partition, offset, and
     * headers — essential for logging and DLT routing.
     */
    @KafkaListener(
            topics          = "${topic.payment-initiated}",
            groupId         = "${spring.kafka.consumer.group-id}",
            containerFactory= "kafkaListenerContainerFactory"
    )
    public void onMessage(
            ConsumerRecord<String, PaymentInitiatedEvent> record,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET)             long offset
    ) {
        PaymentInitiatedEvent event = record.value();

        // ── Null / deserialization guard ──────────────────────────────────────
        // ErrorHandlingDeserializer sets value to null when deserialization
        // fails (rather than throwing). We route null records to the DLT and
        // acknowledge so the partition keeps moving.
        if (event == null) {
            log.error("[CONSUMER] Deserialization failed partition={} offset={} — routing to DLT",
                    partition, offset);
            sendToDlt(record.key(), null, partition, offset,
                    "DeserializationException", "value was null after ErrorHandlingDeserializer");
            acknowledgment.acknowledge();
            return;
        }

        log.info("[CONSUMER] Received eventId={} fromAccount={} toAccount={} amount={} {} partition={} offset={}",
                event.eventId(), event.fromAccount(), event.toAccount(),
                event.amount(), event.currency(), partition, offset);

        // ── Idempotency check ─────────────────────────────────────────────────
        // Kafka guarantees at-least-once delivery. Even with an idempotent
        // producer, a consumer restart or rebalance can redeliver a message.
        // We check if this eventId was already processed and skip if so.
        if (repository.exists(event.eventId())) {
            log.warn("[CONSUMER] Duplicate eventId={} — skipping, acknowledging offset",
                    event.eventId());
            acknowledgment.acknowledge();
            return;
        }

        try {
            // ── Business processing ───────────────────────────────────────────
            processPayment(event, partition, offset);

            // ── Manual commit ─────────────────────────────────────────────────
            // Only commit the offset AFTER processing succeeds.
            // If processPayment() throws, we do NOT reach this line —
            // the offset is not advanced — and the message will be redelivered.
            acknowledgment.acknowledge();

            log.info("[CONSUMER] Processed and acknowledged eventId={} partition={} offset={}",
                    event.eventId(), partition, offset);

        } catch (UnrecoverablePaymentException ex) {
            // ── Poison-pill: route to DLT, acknowledge, keep moving ───────────
            // This error will never succeed on retry (bad data, invalid business
            // rule). Send to DLT so it can be inspected, then commit the offset.
            log.error("[CONSUMER] Unrecoverable error eventId={} error={} — routing to DLT and acknowledging",
                    event.eventId(), ex.getMessage());
            sendToDlt(record.key(), event, partition, offset,
                    ex.getClass().getSimpleName(), ex.getMessage());
            acknowledgment.acknowledge();

        } catch (Exception ex) {
            // ── Transient error: do NOT acknowledge ───────────────────────────
            // Do not commit the offset. The message will be redelivered on the
            // next poll. Log at ERROR level so monitoring alerts fire.
            // If this keeps failing, your consumer lag metric will rise —
            // that's your signal to investigate.
            log.error("[CONSUMER] Transient error eventId={} error={} — will redeliver",
                    event.eventId(), ex.getMessage(), ex);
            // No acknowledgment.acknowledge() here — intentional.
        }
    }

    /**
     * DLT listener — receives messages that failed in the main topic.
     * Logs them for visibility. In production you might persist these
     * to a database or trigger an alert.
     */
    @KafkaListener(
            topics   = "${topic.payment-initiated-dlt}",
            groupId  = "${spring.kafka.consumer.group-id}-dlt"
    )
    public void onDltMessage(
            ConsumerRecord<String, Object> record,
            Acknowledgment acknowledgment
    ) {
        log.warn("[DLT] Received failed record key={} partition={} offset={}",
                record.key(), record.partition(), record.offset());

        // Log all diagnostic headers written by the producer or consumer
        record.headers().forEach(h ->
                log.warn("[DLT]   header {}={}", h.key(), new String(h.value())));

        acknowledgment.acknowledge();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Simulates business processing.
     * Replace this with your actual domain logic (persist to DB, call downstream service etc.)
     */
    private void processPayment(PaymentInitiatedEvent event, int partition, long offset) {
        // Simulate validation — throw UnrecoverablePaymentException for bad data
        if (event.amount() == null || event.amount().signum() <= 0) {
            throw new UnrecoverablePaymentException(
                    "Invalid amount: " + event.amount() + " for eventId=" + event.eventId());
        }
        if (event.fromAccount() == null || event.fromAccount().isBlank()) {
            throw new UnrecoverablePaymentException(
                    "fromAccount is blank for eventId=" + event.eventId());
        }

        // Persist the processed record
        var processed = new ProcessedPayment(
                event.eventId(),
                event.fromAccount(),
                event.toAccount(),
                event.amount(),
                event.currency(),
                event.reference(),
                event.initiatedAt(),
                event.source(),
                partition,
                offset,
                Instant.now(),
                "PROCESSED"
        );

        repository.save(processed);

        log.info("[CONSUMER] Payment processed eventId={} fromAccount={} toAccount={} amount={} {}",
                event.eventId(), event.fromAccount(), event.toAccount(),
                event.amount(), event.currency());
    }

    /**
     * Writes a failed record to the DLT with diagnostic headers.
     * If the DLT write fails, logs DLT_FALLBACK so your log aggregator can alert.
     */
    private void sendToDlt(
            String key,
            PaymentInitiatedEvent event,
            int partition,
            long offset,
            String errorType,
            String errorMessage
    ) {
        try {
            var template = kafkaTemplate;
            var record   = new org.apache.kafka.clients.producer.ProducerRecord<>(
                    dltTopic, key, (Object) event);

            record.headers()
                    .add("x-error-type",       errorType.getBytes())
                    .add("x-error-message",    errorMessage.getBytes())
                    .add("x-source-topic",     "payment-initiated".getBytes())
                    .add("x-source-partition", String.valueOf(partition).getBytes())
                    .add("x-source-offset",    String.valueOf(offset).getBytes())
                    .add("x-failed-at",        Instant.now().toString().getBytes())
                    .add("x-service-id",       "springboot-consumer".getBytes());

            template.send(record);
            log.warn("[DLT] Record routed to DLT key={} errorType={}", key, errorType);

        } catch (Exception dltEx) {
            log.error("DLT_FALLBACK key={} partition={} offset={} originalError={} dltError={}",
                    key, partition, offset, errorMessage, dltEx.getMessage());
        }
    }

    // ── Exception types ───────────────────────────────────────────────────────

    /**
     * Thrown for errors that will never succeed on retry — bad data, failed
     * business rule validation. Routes the record to the DLT immediately.
     */
    public static class UnrecoverablePaymentException extends RuntimeException {
        public UnrecoverablePaymentException(String message) {
            super(message);
        }
    }
}
