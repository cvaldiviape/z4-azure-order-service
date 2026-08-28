package com.z4greed.order.strategy;

import com.z4greed.order.dto.SagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.OrderSagaEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;

@Component
public class OrderSagaStateManager {

  public void update(SagaContextDto sagaContextDto, OrderStatusEnum orderStatusEnum, SagaStatusEnum sagaStatusEnum, EventTypeEnum eventTypeEnum, String errorMessage) {
    LocalDateTime updatedAt = LocalDateTime.now();
    OrderEntity orderEntity = sagaContextDto.orderEntity();

    this.updateOrder(orderEntity, orderStatusEnum, updatedAt);
    this.updateSaga(sagaContextDto, sagaStatusEnum, eventTypeEnum, errorMessage, updatedAt);
  }

  private void updateOrder(OrderEntity orderEntity, OrderStatusEnum orderStatusEnum, LocalDateTime updatedAt) {
    if (orderStatusEnum == null) {
      return;
    }

    orderEntity.setStatus(orderStatusEnum);
    orderEntity.setUpdatedAt(updatedAt);
  }

  private void updateSaga(SagaContextDto sagaContextDto, SagaStatusEnum sagaStatusEnum, EventTypeEnum eventTypeEnum, String errorMessage, LocalDateTime updatedAt) {
    OrderSagaEntity orderSagaEntity = sagaContextDto.orderSagaEntity();

    orderSagaEntity.setStatus(sagaStatusEnum);
    orderSagaEntity.setCurrentStep(eventTypeEnum.getValue());
    orderSagaEntity.setLastEventId(sagaContextDto.sourceEvent().eventId());
    orderSagaEntity.setErrorMessage(errorMessage);
    orderSagaEntity.setUpdatedAt(updatedAt);
  }

}