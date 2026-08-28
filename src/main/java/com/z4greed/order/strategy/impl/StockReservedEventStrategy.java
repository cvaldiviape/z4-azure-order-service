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
public class StockReservedEventStrategy implements OrderSagaEventStrategy {
  private static final String PAYMENTS_TOPIC = "payments.events";

  private final OrderSagaStateManager orderSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OrderEventProducer orderEventProducer;

  public StockReservedEventStrategy(
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
    return EventTypeEnum.STOCK_RESERVED;
  }

  @Override
  public void execute(SagaContextDto sagaContextDto) {
    this.orderSagaStateManager.update(sagaContextDto, OrderStatusEnum.PAYMENT_PENDING, SagaStatusEnum.IN_PROGRESS, EventTypeEnum.STOCK_RESERVED, null);

    OrderEntity orderEntity = sagaContextDto.orderEntity();
    String causationId = sagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = this.buildMapPayload(orderEntity);

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.PAYMENT_REQUESTED, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish(PAYMENTS_TOPIC, eventEnvelopeDto);
  }

  private Map<String, Object> buildMapPayload(OrderEntity orderEntity) {
    return Map.of(
        "customerId", orderEntity.getCustomerId(),
        "amount", orderEntity.getTotalAmount(),
        "currency", orderEntity.getCurrency(),
        "paymentToken", orderEntity.getPaymentToken()
    );
  }

}