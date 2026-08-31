package com.z4greed.order.service.saga.purchase.strategy;

import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.service.saga.purchase.strategy.impl.PaymentApprovedEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.impl.PaymentFailedEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.impl.StockNotAvailableEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.impl.StockReleasedEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.impl.StockReservedEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaEventStrategyRegistry {
  private final Map<EventTypeEnum, PurchaseSagaEventStrategy> mapEventStrategies;

  // Spring inyecta cada estrategia porque todas están registradas como componentes.
  public PurchaseSagaEventStrategyRegistry(
      StockReservedEventStrategy stockReservedEventStrategy,
      StockNotAvailableEventStrategy stockNotAvailableEventStrategy,
      PaymentApprovedEventStrategy paymentApprovedEventStrategy,
      PaymentFailedEventStrategy paymentFailedEventStrategy,
      StockReleasedEventStrategy stockReleasedEventStrategy
  ) {
    // Cada tipo de evento queda asociado explícitamente con la estrategia que debe procesarlo.
    this.mapEventStrategies = Map.of(
        EventTypeEnum.STOCK_RESERVED, stockReservedEventStrategy,
        EventTypeEnum.STOCK_NOT_AVAILABLE, stockNotAvailableEventStrategy,
        EventTypeEnum.PAYMENT_APPROVED, paymentApprovedEventStrategy,
        EventTypeEnum.PAYMENT_FAILED, paymentFailedEventStrategy,
        EventTypeEnum.STOCK_RELEASED, stockReleasedEventStrategy
    );
  }

  public PurchaseSagaEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }

}