package com.z4greed.order.strategy;

import com.z4greed.order.enums.EventTypeEnum;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class OrderSagaEventStrategyRegistry {
  private final Map<EventTypeEnum, OrderSagaEventStrategy> mapEventStrategies;

  public OrderSagaEventStrategyRegistry(List<OrderSagaEventStrategy> listEventStrategies) { // Spring inyecta en esta lista todos los beans que implementan OrderSagaEventStrategy.
    this.mapEventStrategies = new EnumMap<>(EventTypeEnum.class);

    // Cada estrategia se registra por su tipo de evento para localizarla sin condicionales.
    listEventStrategies.forEach(eventStrategy -> {
      EventTypeEnum eventType = eventStrategy.getEventType();
      this.mapEventStrategies.put(eventType, eventStrategy);
    });
  }

  public OrderSagaEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }

}
