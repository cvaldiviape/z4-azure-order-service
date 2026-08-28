package com.z4greed.order.factory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import java.time.LocalDateTime;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class OrderEventFactory {
  private static final Integer EVENT_VERSION = 1;
  private static final String PRODUCER = "order-service";

  private final ObjectMapper mapper;

  public OrderEventFactory(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public EventEnvelopeDto build(EventTypeEnum eventTypeEnum, OrderEntity orderEntity, String causationId, Object payload) {
    String eventId = UUID.randomUUID().toString();
    String aggregateId = orderEntity.getId().toString();
    String correlationId = orderEntity.getCorrelationId();
    JsonNode payloadNode = this.mapper.valueToTree(payload);

    return EventEnvelopeDto.builder()
        .eventId(eventId)
        .eventType(eventTypeEnum.getValue())
        .eventVersion(EVENT_VERSION)
        .aggregateId(aggregateId)
        .correlationId(correlationId)
        .causationId(causationId)
        .timestamp(LocalDateTime.now())
        .producer(PRODUCER)
        .payload(payloadNode)
        .build();
  }

}