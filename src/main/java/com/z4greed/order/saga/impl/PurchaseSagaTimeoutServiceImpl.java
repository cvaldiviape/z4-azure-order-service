package com.z4greed.order.saga.impl;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.entity.PurchaseSagaHistoryEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.repository.PurchaseSagaHistoryRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.saga.PurchaseSagaTimeoutService;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class PurchaseSagaTimeoutServiceImpl implements PurchaseSagaTimeoutService {
  private static final String TIMEOUT_ERROR_PREFIX = "Saga timed out while waiting after step ";

  private final PurchaseSagaRepository purchaseSagaRepository;
  private final PurchaseSagaHistoryRepository purchaseSagaHistoryRepository;

  public PurchaseSagaTimeoutServiceImpl(
      PurchaseSagaRepository purchaseSagaRepository,
      PurchaseSagaHistoryRepository purchaseSagaHistoryRepository
  ) {
    this.purchaseSagaRepository = purchaseSagaRepository;
    this.purchaseSagaHistoryRepository = purchaseSagaHistoryRepository;
  }

  @Override
  @Transactional
  public int timeoutStaleSagas(LocalDateTime cutoff, int batchSize) {
    List<PurchaseSagaEntity> staleSagas = this.purchaseSagaRepository.findStaleForUpdate(cutoff, batchSize);
    staleSagas.forEach(this::markAsTimedOut);
    return staleSagas.size();
  }

  private void markAsTimedOut(PurchaseSagaEntity purchaseSagaEntity) {
    LocalDateTime timedOutAt = LocalDateTime.now();
    String previousStep = purchaseSagaEntity.getCurrentStep();
    String timeoutEventId = "saga-timeout-" + UUID.randomUUID();
    String errorMessage = TIMEOUT_ERROR_PREFIX + previousStep;
    OrderEntity orderEntity = purchaseSagaEntity.getOrder();

    purchaseSagaEntity.setStatus(SagaStatusEnum.TIMED_OUT);
    purchaseSagaEntity.setErrorMessage(errorMessage);
    purchaseSagaEntity.setUpdatedAt(timedOutAt);
    purchaseSagaEntity.setTimedOutAt(timedOutAt);

    PurchaseSagaHistoryEntity historyEntity = PurchaseSagaHistoryEntity.builder()
        .purchaseSaga(purchaseSagaEntity)
        .orderId(orderEntity.getId())
        .orderStatus(orderEntity.getStatus())
        .sagaStatus(SagaStatusEnum.TIMED_OUT)
        .eventType(EventTypeEnum.SAGA_TIMED_OUT)
        .eventId(timeoutEventId)
        .errorMessage(errorMessage)
        .createdAt(timedOutAt)
        .build();

    this.purchaseSagaHistoryRepository.save(historyEntity);
    log.warn(
        "action=saga_timed_out eventType={} eventId={} correlationId={} orderId={} previousStep={} orderStatus={} sagaStatus={}",
        EventTypeEnum.SAGA_TIMED_OUT.getValue(),
        timeoutEventId,
        orderEntity.getCorrelationId(),
        orderEntity.getId(),
        previousStep,
        orderEntity.getStatus(),
        SagaStatusEnum.TIMED_OUT);
  }
}
