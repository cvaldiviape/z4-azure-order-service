package com.z4greed.order.strategy;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.strategy.impl.StockNotAvailableEventStrategy;
import com.z4greed.order.strategy.impl.StockReleasedEventStrategy;
import com.z4greed.order.strategy.impl.StockReservedEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockEventStrategyRegistry implements EventStrategyRegistry<EventTypeEnum, PurchaseSagaContextDto> {
  private final Map<EventTypeEnum, EventStrategy<EventTypeEnum, PurchaseSagaContextDto>> mapEventStrategies;

  public StockEventStrategyRegistry(
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
  public EventStrategy<EventTypeEnum, PurchaseSagaContextDto> find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }

}