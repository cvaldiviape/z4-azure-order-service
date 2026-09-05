package com.z4greed.order.service.saga.purchase.strategy;

import com.z4greed.order.enums.EventTypeEnum;

public interface PurchaseSagaEventStrategyRegistry {
  PurchaseSagaEventStrategy find(EventTypeEnum eventTypeEnum);
}
