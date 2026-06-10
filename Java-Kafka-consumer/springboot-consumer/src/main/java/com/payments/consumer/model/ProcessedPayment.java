package com.payments.consumer.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents a successfully processed payment event.
 * Stored in-memory so the REST API can show what has been consumed.
 * In production this would be persisted to a database.
 */
public record ProcessedPayment(

        String  eventId,
        String  fromAccount,
        String  toAccount,
        BigDecimal amount,
        String  currency,
        String  reference,
        Instant initiatedAt,
        String  source,

        // Consumer-side metadata
        int     partition,
        long    offset,
        Instant processedAt,
        String  status         // PROCESSED | FAILED
) {}
