package com.z4greed.order.saga.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.entity.PurchaseSagaHistoryEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.repository.PurchaseSagaHistoryRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PurchaseSagaTimeoutServiceImplTests {
  @Mock private PurchaseSagaRepository purchaseSagaRepository;
  @Mock private PurchaseSagaHistoryRepository purchaseSagaHistoryRepository;
  @InjectMocks private PurchaseSagaTimeoutServiceImpl timeoutService;

  @Test
  void shouldMarkStaleSagaAsTimedOutWithoutChangingRecoverableOrderData() {
    LocalDateTime cutoff = LocalDateTime.now().minusMinutes(5);
    OrderEntity orderEntity = OrderEntity.builder()
        .id(10L)
        .status(OrderStatusEnum.PAYMENT_PENDING)
        .correlationId("purchase-10")
        .paymentToken("temporary-token")
        .build();
    PurchaseSagaEntity sagaEntity = PurchaseSagaEntity.builder()
        .id(20L)
        .order(orderEntity)
        .status(SagaStatusEnum.IN_PROGRESS)
        .currentStep(EventTypeEnum.STOCK_RESERVED.getValue())
        .build();

    when(this.purchaseSagaRepository.findStaleForUpdate(cutoff, 100)).thenReturn(List.of(sagaEntity));

    int result = this.timeoutService.timeoutStaleSagas(cutoff, 100);

    assertThat(result).isEqualTo(1);
    assertThat(sagaEntity.getStatus()).isEqualTo(SagaStatusEnum.TIMED_OUT);
    assertThat(sagaEntity.getCurrentStep()).isEqualTo(EventTypeEnum.SAGA_TIMED_OUT.getValue());
    assertThat(sagaEntity.getErrorMessage()).contains(EventTypeEnum.STOCK_RESERVED.getValue());
    assertThat(orderEntity.getStatus()).isEqualTo(OrderStatusEnum.PAYMENT_PENDING);
    assertThat(orderEntity.getPaymentToken()).isEqualTo("temporary-token");

    ArgumentCaptor<PurchaseSagaHistoryEntity> historyCaptor = ArgumentCaptor.forClass(PurchaseSagaHistoryEntity.class);
    verify(this.purchaseSagaHistoryRepository).save(historyCaptor.capture());
    PurchaseSagaHistoryEntity historyEntity = historyCaptor.getValue();
    assertThat(historyEntity.getSagaStatus()).isEqualTo(SagaStatusEnum.TIMED_OUT);
    assertThat(historyEntity.getEventType()).isEqualTo(EventTypeEnum.SAGA_TIMED_OUT);
    assertThat(historyEntity.getOrderStatus()).isEqualTo(OrderStatusEnum.PAYMENT_PENDING);
    assertThat(historyEntity.getEventId()).startsWith("saga-timeout-");
  }
}
