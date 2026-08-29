package com.z4greed.order.service.saga.purchase.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.service.saga.purchase.strategy.PurchaseSagaEventStrategy;
import com.z4greed.order.service.saga.purchase.state.PurchaseSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockReleasedEventStrategy implements PurchaseSagaEventStrategy {
  private static final String ORDERS_TOPIC = "orders-events-topic";
  private static final String CANCELLATION_REASON = "Payment failed";

  private final PurchaseSagaStateManager purchaseSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OrderEventProducer orderEventProducer;

  public StockReleasedEventStrategy(
      PurchaseSagaStateManager purchaseSagaStateManager,
      OrderEventFactory orderEventFactory,
      OrderEventProducer orderEventProducer
  ) {
    this.purchaseSagaStateManager = purchaseSagaStateManager;
    this.orderEventFactory = orderEventFactory;
    this.orderEventProducer = orderEventProducer;
  }

  @Override
  public EventTypeEnum getEventType() {
    return EventTypeEnum.STOCK_RELEASED;
  }

  @Override
  public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
    this.purchaseSagaStateManager.update(purchaseSagaContextDto, OrderStatusEnum.CANCELLED, SagaStatusEnum.COMPLETED, EventTypeEnum.ORDER_CANCELLED, CANCELLATION_REASON);

    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
    String causationId = purchaseSagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = Map.of("reason", CANCELLATION_REASON);

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.ORDER_CANCELLED, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish(ORDERS_TOPIC, eventEnvelopeDto);
  }

}