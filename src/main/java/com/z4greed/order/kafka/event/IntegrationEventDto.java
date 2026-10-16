package com.z4greed.order.kafka.event;

import tools.jackson.databind.JsonNode;
import java.time.LocalDateTime;
import lombok.Builder;

@Builder
public record IntegrationEventDto(
    String eventId,
    String eventType,
    Integer eventVersion,
    String aggregateId,
    String correlationId,
    String previousEventId,
    LocalDateTime timestamp,
    String producer,
    JsonNode payload) {}
