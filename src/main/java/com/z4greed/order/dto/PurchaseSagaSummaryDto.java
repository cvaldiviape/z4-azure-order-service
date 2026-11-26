package com.z4greed.order.dto;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import java.time.LocalDateTime;

// Estado actual de la Saga de un pedido, para la lista de la Ops Console.
public record PurchaseSagaSummaryDto(
    Long orderId,
    String correlationId,
    OrderStatusEnum orderStatus,
    SagaStatusEnum sagaStatus,
    String currentStep,
    String errorMessage,
    LocalDateTime createdAt,
    LocalDateTime updatedAt,
    LocalDateTime timedOutAt) {

    public static PurchaseSagaSummaryDto from(PurchaseSagaEntity purchaseSagaEntity) {
        OrderEntity orderEntity = purchaseSagaEntity.getOrder();
        return new PurchaseSagaSummaryDto(
                orderEntity.getId(),
                orderEntity.getCorrelationId(),
                orderEntity.getStatus(),
                purchaseSagaEntity.getStatus(),
                purchaseSagaEntity.getCurrentStep(),
                purchaseSagaEntity.getErrorMessage(),
                purchaseSagaEntity.getCreatedAt(),
                purchaseSagaEntity.getUpdatedAt(),
                purchaseSagaEntity.getTimedOutAt());
    }
}
