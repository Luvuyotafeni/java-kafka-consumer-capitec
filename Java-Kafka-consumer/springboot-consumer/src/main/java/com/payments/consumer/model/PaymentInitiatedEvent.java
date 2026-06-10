package com.payments.consumer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Inbound event deserialized from the payment-initiated topic.
 *
 * @JsonIgnoreProperties(ignoreUnknown = true) is critical here:
 * it means this consumer will tolerate new fields added by the producer
 * (BACKWARD compatible schema evolution) without throwing a deserialization error.
 * Never omit this annotation on a consumer model.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentInitiatedEvent(

        @JsonProperty("eventId")
        String eventId,

        @JsonProperty("fromAccount")
        String fromAccount,

        @JsonProperty("toAccount")
        String toAccount,

        @JsonProperty("amount")
        BigDecimal amount,

        @JsonProperty("currency")
        String currency,

        @JsonProperty("reference")
        String reference,

        @JsonProperty("initiatedAt")
        Instant initiatedAt,

        @JsonProperty("source")
        String source
) {}
