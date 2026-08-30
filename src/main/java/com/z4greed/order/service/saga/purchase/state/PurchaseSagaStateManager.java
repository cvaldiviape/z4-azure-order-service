package com.z4greed.order.service.saga.purchase.state;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.entity.PurchaseSagaHistoryEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.repository.PurchaseSagaHistoryRepository;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaStateManager {
  private final PurchaseSagaHistoryRepository purchaseSagaHistoryRepository;

  public PurchaseSagaStateManager(PurchaseSagaHistoryRepository purchaseSagaHistoryRepository) {
    this.purchaseSagaHistoryRepository = purchaseSagaHistoryRepository;
  }

  public void update(PurchaseSagaContextDto purchaseSagaContextDto, OrderStatusEnum orderStatusEnum, SagaStatusEnum sagaStatusEnum, EventTypeEnum eventTypeEnum, String errorMessage) {
    LocalDateTime updatedAt = LocalDateTime.now();
    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();

    this.updateOrder(orderEntity, orderStatusEnum, updatedAt);
    this.updateSaga(purchaseSagaContextDto, sagaStatusEnum, eventTypeEnum, errorMessage, updatedAt);
    this.createPurchaseSagaHistory(purchaseSagaContextDto, eventTypeEnum, orderEntity, updatedAt);
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

  private void createPurchaseSagaHistory(PurchaseSagaContextDto purchaseSagaContextDto, EventTypeEnum eventTypeEnum, OrderEntity orderEntity, LocalDateTime updatedAt) {
    PurchaseSagaEntity purchaseSagaEntity = purchaseSagaContextDto.purchaseSagaEntity();
    String eventId = purchaseSagaContextDto.sourceEvent().eventId();

    PurchaseSagaHistoryEntity purchaseSagaHistoryEntity = PurchaseSagaHistoryEntity.builder()
            .purchaseSaga(purchaseSagaEntity)
            .orderId(orderEntity.getId())
            .orderStatus(orderEntity.getStatus())
            .sagaStatus(purchaseSagaEntity.getStatus())
            .eventType(eventTypeEnum)
            .eventId(eventId)
            .errorMessage(purchaseSagaEntity.getErrorMessage())
            .createdAt(updatedAt)
            .build();

    this.purchaseSagaHistoryRepository.save(purchaseSagaHistoryEntity);
  }

}