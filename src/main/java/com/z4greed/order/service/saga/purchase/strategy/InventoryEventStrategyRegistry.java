package com.z4greed.order.service.saga.purchase.strategy;

import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.service.saga.purchase.strategy.impl.StockNotAvailableEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.impl.StockReleasedEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.impl.StockReservedEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventStrategyRegistry implements PurchaseSagaEventStrategyRegistry {
  private final Map<EventTypeEnum, PurchaseSagaEventStrategy> mapEventStrategies;

  public InventoryEventStrategyRegistry(
      StockReservedEventStrategy stockReservedEventStrategy,
      StockNotAvailableEventStrategy stockNotAvailableEventStrategy,
      StockReleasedEventStrategy stockReleasedEventStrategy
  ) {
    this.mapEventStrategies = Map.of(
        EventTypeEnum.STOCK_RESERVED, stockReservedEventStrategy,
        EventTypeEnum.STOCK_NOT_AVAILABLE, stockNotAvailableEventStrategy,
        EventTypeEnum.STOCK_RELEASED, stockReleasedEventStrategy
    );
  }

  @Override
  public PurchaseSagaEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }
}
