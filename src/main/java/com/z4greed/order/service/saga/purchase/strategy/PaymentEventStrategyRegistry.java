package com.z4greed.order.service.saga.purchase.strategy;

import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.service.saga.purchase.strategy.impl.PaymentApprovedEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.impl.PaymentFailedEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventStrategyRegistry implements PurchaseSagaEventStrategyRegistry {
  private final Map<EventTypeEnum, PurchaseSagaEventStrategy> mapEventStrategies;

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
  public PurchaseSagaEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }
}
