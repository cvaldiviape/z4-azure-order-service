package com.z4greed.order.kafka.event;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import lombok.Builder;

@Builder
public record EventEnvelopeDto(
    String eventId,
    String eventType,
    int eventVersion,
    String aggregateId,
    String correlationId,
    String causationId,
    Instant timestamp,
    String producer,
    JsonNode payload) {}
