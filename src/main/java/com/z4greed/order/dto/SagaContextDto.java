package com.z4greed.order.dto;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.OrderSagaEntity;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import lombok.Builder;

@Builder
public record SagaContextDto(
    OrderEntity orderEntity,
    OrderSagaEntity orderSagaEntity,
    EventEnvelopeDto sourceEvent) {}
