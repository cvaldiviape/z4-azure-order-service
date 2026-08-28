package com.z4greed.order.strategy;

import com.z4greed.order.dto.SagaContextDto;
import com.z4greed.order.enums.EventTypeEnum;

public interface OrderSagaEventStrategy {
  EventTypeEnum getEventType();
  void execute(SagaContextDto sagaContextDto);
}
