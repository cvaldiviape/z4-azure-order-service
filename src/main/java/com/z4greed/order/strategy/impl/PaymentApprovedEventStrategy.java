package com.z4greed.order.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.strategy.EventStrategy;
import com.z4greed.order.saga.state.PurchaseSagaStateManager;
import org.springframework.stereotype.Component;

@Component
public class PaymentApprovedEventStrategy implements EventStrategy<EventTypeEnum, PurchaseSagaContextDto> {
  private final PurchaseSagaStateManager purchaseSagaStateManager;

  public PaymentApprovedEventStrategy(PurchaseSagaStateManager purchaseSagaStateManager) {
    this.purchaseSagaStateManager = purchaseSagaStateManager;
  }

  @Override
  public EventTypeEnum getEventType() {
    return EventTypeEnum.PAYMENT_APPROVED;
  }

  @Override
  public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
    this.purchaseSagaStateManager.update(purchaseSagaContextDto, OrderStatusEnum.CONFIRMED, SagaStatusEnum.COMPLETED, EventTypeEnum.PAYMENT_APPROVED, null);
  }

}