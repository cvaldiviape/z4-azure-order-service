package com.z4greed.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.OrderSagaEntity;
import com.z4greed.order.entity.ProcessedEventEntity;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.ProcessedEventRepository;
import com.z4greed.order.repository.SagaRepository;
import java.util.Map;
import java.util.function.Consumer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class OrderSagaService {
  private final OrderRepository orderRepository;
  private final SagaRepository sagaRepository;
  private final ProcessedEventRepository processedEventRepository;
  private final OrderEventProducer orderEventProducer;
  private final OrderService orderService;
  private final ObjectMapper objectMapper;
  private final Map<EventTypeEnum, Consumer<SagaContext>> mapEventHandlers;

  public OrderSagaService(OrderRepository orderRepository, SagaRepository sagaRepository,
      ProcessedEventRepository processedEventRepository, OrderEventProducer orderEventProducer,
      OrderService orderService, ObjectMapper objectMapper) {
    this.orderRepository = orderRepository;
    this.sagaRepository = sagaRepository;
    this.processedEventRepository = processedEventRepository;
    this.orderEventProducer = orderEventProducer;
    this.orderService = orderService;
    this.objectMapper = objectMapper;
    this.mapEventHandlers = Map.of(
        EventTypeEnum.STOCK_RESERVED, this::requestPayment,
        EventTypeEnum.STOCK_NOT_AVAILABLE, context -> this.cancelOrder(context, "Stock not available"),
        EventTypeEnum.STOCK_RELEASED, context -> this.cancelOrder(context, "Payment failed"),
        EventTypeEnum.PAYMENT_APPROVED, this::confirmOrder,
        EventTypeEnum.PAYMENT_FAILED, this::releaseStock);
  }

  public void handleInventoryEvent(String rawEvent) {
    this.handleEvent(rawEvent);
  }

  public void handlePaymentEvent(String rawEvent) {
    this.handleEvent(rawEvent);
  }

  private void handleEvent(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    if (this.wasProcessed(eventEnvelopeDto)) {
      return;
    }

    Consumer<SagaContext> eventHandler = EventTypeEnum.fromValue(eventEnvelopeDto.eventType())
        .map(this.mapEventHandlers::get)
        .orElse(null);
    if (eventHandler == null) {
      return;
    }

    SagaContext sagaContext = this.loadSagaContext(eventEnvelopeDto);
    eventHandler.accept(sagaContext);
    this.markAsProcessed(eventEnvelopeDto);
  }

  private boolean wasProcessed(EventEnvelopeDto eventEnvelopeDto) {
    return this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
  }

  private SagaContext loadSagaContext(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    OrderEntity orderEntity = this.orderRepository.findById(orderId).orElseThrow();
    OrderSagaEntity orderSagaEntity = this.sagaRepository.findByOrderId(orderId).orElseThrow();
    return new SagaContext(orderEntity, orderSagaEntity, eventEnvelopeDto);
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = new ProcessedEventEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }

  private void requestPayment(SagaContext context) {
    context.orderEntity().changeStatus(OrderStatusEnum.PAYMENT_PENDING);
    context.orderSagaEntity().transition(SagaStatusEnum.IN_PROGRESS, EventTypeEnum.STOCK_RESERVED.getValue(), context.sourceEvent().eventId());
    Map<String, Object> mapPayload = Map.of("customerId", context.orderEntity().getCustomerId(),
        "amount", context.orderEntity().getTotalAmount(), "currency", context.orderEntity().getCurrency(),
        "paymentToken", context.orderEntity().getPaymentToken());
    EventEnvelopeDto eventEnvelopeDto = this.orderService.createEvent(EventTypeEnum.PAYMENT_REQUESTED.getValue(),
        context.orderEntity(), context.sourceEvent().eventId(), mapPayload);
    this.orderEventProducer.publish("payments.events", eventEnvelopeDto);
  }

  private void confirmOrder(SagaContext context) {
    context.orderEntity().changeStatus(OrderStatusEnum.CONFIRMED);
    context.orderSagaEntity().transition(SagaStatusEnum.COMPLETED, EventTypeEnum.PAYMENT_APPROVED.getValue(),
        context.sourceEvent().eventId());
    Map<String, Object> mapPayload = Map.of();
    EventEnvelopeDto eventEnvelopeDto = this.orderService.createEvent(EventTypeEnum.ORDER_CONFIRMED.getValue(),
        context.orderEntity(), context.sourceEvent().eventId(), mapPayload);
    this.orderEventProducer.publish("orders.events", eventEnvelopeDto);
  }

  private void releaseStock(SagaContext context) {
    context.orderSagaEntity().transition(SagaStatusEnum.COMPENSATING, EventTypeEnum.PAYMENT_FAILED.getValue(),
        context.sourceEvent().eventId());
    Map<String, Object> mapPayload = Map.of();
    EventEnvelopeDto eventEnvelopeDto = this.orderService.createEvent(EventTypeEnum.RELEASE_STOCK.getValue(),
        context.orderEntity(), context.sourceEvent().eventId(), mapPayload);
    this.orderEventProducer.publish("inventory.events", eventEnvelopeDto);
  }

  private void cancelOrder(SagaContext context, String reason) {
    context.orderEntity().changeStatus(OrderStatusEnum.CANCELLED);
    context.orderSagaEntity().completeCancellation(context.sourceEvent().eventId(), reason);
    Map<String, Object> mapPayload = Map.of("reason", reason);
    EventEnvelopeDto eventEnvelopeDto = this.orderService.createEvent(EventTypeEnum.ORDER_CANCELLED.getValue(),
        context.orderEntity(), context.sourceEvent().eventId(), mapPayload);
    this.orderEventProducer.publish("orders.events", eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.objectMapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private record SagaContext(OrderEntity orderEntity, OrderSagaEntity orderSagaEntity,
      EventEnvelopeDto sourceEvent) {}
}
