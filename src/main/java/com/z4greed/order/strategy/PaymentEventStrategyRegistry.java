package com.z4greed.order.strategy;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.strategy.impl.PaymentApprovedEventStrategy;
import com.z4greed.order.strategy.impl.PaymentFailedEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventStrategyRegistry implements EventStrategyRegistry<EventTypeEnum, PurchaseSagaContextDto> {
  private final Map<EventTypeEnum, EventStrategy<EventTypeEnum, PurchaseSagaContextDto>> mapEventStrategies;

  public PaymentEventStrategyRegistry(
      PaymentApprovedEventStrategy paymentApprovedEventStrategy,
      PaymentFailedEventStrategy paymentFailedEventStrategy
  ) {
    this.mapEventStrategies = Map.of(
        EventTypeEnum.PAYMENT_APPROVED, paymentApprovedEventStrategy,
        EventTypeEnum.PAYMENT_FAILED, paymentFailedEventStrategy
    );
  }

  @Override
  public EventStrategy<EventTypeEnum, PurchaseSagaContextDto> find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }

}