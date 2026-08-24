package com.z4greed.order.kafka.event;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDateTime;
import lombok.Builder;

@Builder
public record EventEnvelopeDto(
    String eventId,
    String eventType,
    Integer eventVersion,
    String aggregateId,
    String correlationId,
    String causationId,
    LocalDateTime timestamp,
    String producer,
    JsonNode payload) {}
