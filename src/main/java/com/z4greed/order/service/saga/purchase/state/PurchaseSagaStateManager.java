package com.z4greed.order.service.saga.purchase.state;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaStateManager {

  public void update(PurchaseSagaContextDto purchaseSagaContextDto, OrderStatusEnum orderStatusEnum, SagaStatusEnum sagaStatusEnum, EventTypeEnum eventTypeEnum, String errorMessage) {
    LocalDateTime updatedAt = LocalDateTime.now();
    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();

    this.updateOrder(orderEntity, orderStatusEnum, updatedAt);
    this.updateSaga(purchaseSagaContextDto, sagaStatusEnum, eventTypeEnum, errorMessage, updatedAt);
  }

  private void updateOrder(OrderEntity orderEntity, OrderStatusEnum orderStatusEnum, LocalDateTime updatedAt) {
    if (orderStatusEnum == null) {
      return;
    }

    orderEntity.setStatus(orderStatusEnum);
    orderEntity.setUpdatedAt(updatedAt);
  }

  private void updateSaga(PurchaseSagaContextDto purchaseSagaContextDto, SagaStatusEnum sagaStatusEnum, EventTypeEnum eventTypeEnum, String errorMessage, LocalDateTime updatedAt) {
    PurchaseSagaEntity purchaseSagaEntity = purchaseSagaContextDto.purchaseSagaEntity();

    purchaseSagaEntity.setStatus(sagaStatusEnum);
    purchaseSagaEntity.setCurrentStep(eventTypeEnum.getValue());
    purchaseSagaEntity.setLastEventId(purchaseSagaContextDto.sourceEvent().eventId());
    purchaseSagaEntity.setErrorMessage(errorMessage);
    purchaseSagaEntity.setUpdatedAt(updatedAt);
  }

}