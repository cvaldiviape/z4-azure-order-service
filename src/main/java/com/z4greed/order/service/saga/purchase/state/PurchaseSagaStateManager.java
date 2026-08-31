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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
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
    this.logSagaState(purchaseSagaContextDto, eventTypeEnum, orderEntity);
  }

  private void logSagaState(PurchaseSagaContextDto purchaseSagaContextDto, EventTypeEnum eventTypeEnum, OrderEntity orderEntity) {
    PurchaseSagaEntity purchaseSagaEntity = purchaseSagaContextDto.purchaseSagaEntity();
    String correlationId = purchaseSagaContextDto.sourceEvent().correlationId();
    String errorMessage = purchaseSagaEntity.getErrorMessage();

    if (errorMessage == null) {
      log.info("action=saga_state_changed eventType={} eventId={} correlationId={} orderId={} orderStatus={} sagaStatus={}", eventTypeEnum.getValue(), purchaseSagaContextDto.sourceEvent().eventId(), correlationId, orderEntity.getId(), orderEntity.getStatus(), purchaseSagaEntity.getStatus());
      return;
    }

    log.info("action=saga_state_changed eventType={} eventId={} correlationId={} orderId={} orderStatus={} sagaStatus={} errorMessage=\"{}\"", eventTypeEnum.getValue(), purchaseSagaContextDto.sourceEvent().eventId(), correlationId, orderEntity.getId(), orderEntity.getStatus(), purchaseSagaEntity.getStatus(), errorMessage);
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
