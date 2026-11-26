package com.z4greed.order.dto;

import com.z4greed.order.entity.PurchaseSagaHistoryEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import java.time.LocalDateTime;

// Un paso de la línea de tiempo de la Saga.
public record PurchaseSagaHistoryDto(
    EventTypeEnum eventType,
    String eventId,
    OrderStatusEnum orderStatus,
    SagaStatusEnum sagaStatus,
    String errorMessage,
    LocalDateTime createdAt) {

    public static PurchaseSagaHistoryDto from(PurchaseSagaHistoryEntity historyEntity) {
        return new PurchaseSagaHistoryDto(
                historyEntity.getEventType(),
                historyEntity.getEventId(),
                historyEntity.getOrderStatus(),
                historyEntity.getSagaStatus(),
                historyEntity.getErrorMessage(),
                historyEntity.getCreatedAt());
    }
}
