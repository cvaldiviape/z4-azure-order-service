package com.z4greed.order.service.saga.purchase.strategy;

import com.z4greed.order.enums.EventTypeEnum;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaEventStrategyRegistry {
  private final Map<EventTypeEnum, PurchaseSagaEventStrategy> mapEventStrategies;

  public PurchaseSagaEventStrategyRegistry(List<PurchaseSagaEventStrategy> listEventStrategies) { // Spring inyecta todos los beans que implementan PurchaseSagaEventStrategy.
    this.mapEventStrategies = new EnumMap<>(EventTypeEnum.class);

    // Cada estrategia se registra por su tipo de evento para localizarla sin condicionales.
    listEventStrategies.forEach(eventStrategy -> {
      EventTypeEnum eventType = eventStrategy.getEventType();
      this.mapEventStrategies.put(eventType, eventStrategy);
    });
  }

  public PurchaseSagaEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }

}