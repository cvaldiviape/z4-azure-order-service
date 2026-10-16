package com.z4greed.order.dto;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.kafka.event.IntegrationEventDto;
import lombok.Builder;

@Builder
public record PurchaseSagaContextDto(
    OrderEntity orderEntity,
    PurchaseSagaEntity purchaseSagaEntity,
    IntegrationEventDto event
) {}
