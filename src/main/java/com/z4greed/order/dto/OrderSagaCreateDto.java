package com.z4greed.order.dto;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.SagaStatusEnum;
import java.time.Instant;
import lombok.Builder;

@Builder
public record OrderSagaCreateDto(
    OrderEntity order,
    SagaStatusEnum status,
    String currentStep,
    String lastEventId,
    String errorMessage,
    Instant createdAt,
    Instant updatedAt) {}
