package com.z4greed.order.strategy.impl;

import com.z4greed.order.dto.SagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.strategy.OrderSagaEventStrategy;
import com.z4greed.order.strategy.OrderSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedEventStrategy implements OrderSagaEventStrategy {
  private static final String INVENTORY_TOPIC = "inventory.events";

  private final OrderSagaStateManager orderSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OrderEventProducer orderEventProducer;

  public PaymentFailedEventStrategy(
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
    return EventTypeEnum.PAYMENT_FAILED;
  }

  @Override
  public void execute(SagaContextDto sagaContextDto) {
    this.orderSagaStateManager.update(sagaContextDto, null, SagaStatusEnum.COMPENSATING, EventTypeEnum.PAYMENT_FAILED, null);

    OrderEntity orderEntity = sagaContextDto.orderEntity();
    String causationId = sagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = Map.of();

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.RELEASE_STOCK, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish(INVENTORY_TOPIC, eventEnvelopeDto);
  }

}