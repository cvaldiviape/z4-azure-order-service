package com.z4greed.order.service.saga.purchase.strategy;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.enums.EventTypeEnum;

public interface PurchaseSagaEventStrategy {
  EventTypeEnum getEventType();
  void execute(PurchaseSagaContextDto purchaseSagaContextDto);
}