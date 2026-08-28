package com.z4greed.order.strategy.impl;

import com.z4greed.order.dto.SagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.strategy.OrderSagaEventStrategy;
import com.z4greed.order.strategy.OrderSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockNotAvailableEventStrategy implements OrderSagaEventStrategy {
  private static final String ORDERS_TOPIC = "orders.events";
  private static final String CANCELLATION_REASON = "Stock not available";

  private final OrderSagaStateManager orderSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OrderEventProducer orderEventProducer;

  public StockNotAvailableEventStrategy(
      OrderSagaStateManager orderSagaStateManager,
      OrderEventFactory orderEventFactory,
      OrderEventProducer orderEventProducer
  ) {
    this.orderSagaStateManager = orderSagaStateManager;
    this.orderEventFactory = orderEventFactory;
    this.orderEventProducer = orderEventProducer;
  }

  @Override
  public EventTypeEnum getEventType() {
    return EventTypeEnum.STOCK_NOT_AVAILABLE;
  }

  @Override
  public void execute(SagaContextDto sagaContextDto) {
    this.orderSagaStateManager.update(sagaContextDto, OrderStatusEnum.CANCELLED, SagaStatusEnum.COMPLETED, EventTypeEnum.ORDER_CANCELLED, CANCELLATION_REASON);

    OrderEntity orderEntity = sagaContextDto.orderEntity();
    String causationId = sagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = Map.of("reason", CANCELLATION_REASON);

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.ORDER_CANCELLED, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish(ORDERS_TOPIC, eventEnvelopeDto);
  }

}