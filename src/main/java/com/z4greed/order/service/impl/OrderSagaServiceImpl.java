package com.z4greed.order.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.order.dto.SagaContextDto;
import com.z4greed.order.entity.*;
import com.z4greed.order.enums.*;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.mapper.ProcessedEventMapper;
import com.z4greed.order.repository.*;
import com.z4greed.order.service.OrderSagaService;
import com.z4greed.order.service.OrderService;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class OrderSagaServiceImpl implements OrderSagaService {
  private final OrderRepository orderRepository;
  private final SagaRepository sagaRepository;
  private final ProcessedEventRepository processedEventRepository;
  private final OrderEventProducer orderEventProducer;
  private final OrderService orderService;
  private final ProcessedEventMapper processedEventMapper;
  private final ObjectMapper mapper;

  public OrderSagaServiceImpl(
      OrderRepository orderRepository,
      SagaRepository sagaRepository,
      ProcessedEventRepository processedEventRepository,
      OrderEventProducer orderEventProducer,
      OrderService orderService,
      ProcessedEventMapper processedEventMapper,
      ObjectMapper mapper
  ) {
    this.orderRepository = orderRepository;
    this.sagaRepository = sagaRepository;
    this.processedEventRepository = processedEventRepository;
    this.orderEventProducer = orderEventProducer;
    this.orderService = orderService;
    this.processedEventMapper = processedEventMapper;
    this.mapper = mapper;
  }

  @Override
  public void handleInventoryEvent(String rawEvent) {
    this.handleEvent(rawEvent);
  }

  @Override
  public void handlePaymentEvent(String rawEvent) {
    this.handleEvent(rawEvent);
  }

  private void handleEvent(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    Boolean wasProcessed = this.wasProcessed(eventEnvelopeDto);

    if (wasProcessed) {
      return;
    }

    Consumer<SagaContextDto> eventHandler = this.findEventHandler(eventEnvelopeDto);

    if (eventHandler == null) {
      return;
    }

    SagaContextDto sagaContextDto = this.loadSagaContext(eventEnvelopeDto);
    eventHandler.accept(sagaContextDto);

    this.markAsProcessed(eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private Boolean wasProcessed(EventEnvelopeDto eventEnvelopeDto) {
    return this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
  }

  private Consumer<SagaContextDto> findEventHandler(EventEnvelopeDto eventEnvelopeDto) {
    Map<EventTypeEnum, Consumer<SagaContextDto>> mapEventHandlers = Map.of(
        EventTypeEnum.STOCK_RESERVED, this::requestPayment,
        EventTypeEnum.STOCK_NOT_AVAILABLE, contextDto -> this.cancelOrder(contextDto, "Stock not available"),
        EventTypeEnum.STOCK_RELEASED, contextDto -> this.cancelOrder(contextDto, "Payment failed"),
        EventTypeEnum.PAYMENT_APPROVED, this::confirmOrder,
        EventTypeEnum.PAYMENT_FAILED, this::releaseStock
    );

    String eventType = eventEnvelopeDto.eventType();

    return EventTypeEnum.fromValue(eventType)
        .map(mapEventHandlers::get)
        .orElse(null);
  }

  private void requestPayment(SagaContextDto contextDto) {
    this.updateSaga(contextDto, OrderStatusEnum.PAYMENT_PENDING, SagaStatusEnum.IN_PROGRESS, EventTypeEnum.STOCK_RESERVED, null);

    String eventYpe = EventTypeEnum.PAYMENT_REQUESTED.getValue();
    OrderEntity orderEntity = contextDto.orderEntity();
    String causationId = contextDto.sourceEvent().eventId();

    Map<String, Object> mapPayload = this.buildMapPayload(orderEntity);

    EventEnvelopeDto eventEnvelopeDto = this.orderService.buildEvent(eventYpe, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish("payments.events", eventEnvelopeDto);
  }

  private Map<String, Object> buildMapPayload(OrderEntity orderEntity) {
    return Map.of(
            "customerId", orderEntity.getCustomerId(),
            "amount", orderEntity.getTotalAmount(),
            "currency", orderEntity.getCurrency(),
            "paymentToken", orderEntity.getPaymentToken()
    );
  }

  private void cancelOrder(SagaContextDto contextDto, String reason) {
    this.updateSaga(contextDto, OrderStatusEnum.CANCELLED, SagaStatusEnum.COMPLETED, EventTypeEnum.ORDER_CANCELLED, reason);
    Map<String, Object> mapPayload = Map.of("reason", reason);
    this.publishOrderEvent(contextDto, EventTypeEnum.ORDER_CANCELLED, mapPayload);
  }

  private void publishOrderEvent(SagaContextDto contextDto, EventTypeEnum eventType, Map<String, Object> mapPayload) {
    OrderEntity orderEntity = contextDto.orderEntity();
    String causationId = contextDto.sourceEvent().eventId();

    EventEnvelopeDto eventEnvelopeDto = this.orderService.buildEvent(eventType.getValue(), orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish("orders.events", eventEnvelopeDto);
  }

  private void confirmOrder(SagaContextDto contextDto) {
    this.updateSaga(contextDto, OrderStatusEnum.CONFIRMED, SagaStatusEnum.COMPLETED, EventTypeEnum.PAYMENT_APPROVED, null);
    this.publishOrderEvent(contextDto, EventTypeEnum.ORDER_CONFIRMED, Map.of());
  }

  private void releaseStock(SagaContextDto contextDto) {
    this.updateSaga(contextDto, null, SagaStatusEnum.COMPENSATING, EventTypeEnum.PAYMENT_FAILED, null);

    String eventType = EventTypeEnum.RELEASE_STOCK.getValue();
    OrderEntity orderEntity = contextDto.orderEntity();
    String causationId = contextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = Map.of();

    EventEnvelopeDto eventEnvelopeDto = this.orderService.buildEvent(eventType, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish("inventory.events", eventEnvelopeDto);
  }

  private void updateSaga(SagaContextDto contextDto, OrderStatusEnum orderStatus, SagaStatusEnum sagaStatus, EventTypeEnum eventType, String errorMessage) {
    LocalDateTime updatedAt = LocalDateTime.now();

    if (orderStatus != null) {
      OrderEntity orderEntity = contextDto.orderEntity();
      orderEntity.setStatus(orderStatus);
      orderEntity.setUpdatedAt(updatedAt);
    }

    OrderSagaEntity orderSagaEntity = contextDto.orderSagaEntity();
    orderSagaEntity.setStatus(sagaStatus);
    orderSagaEntity.setCurrentStep(eventType.getValue());
    orderSagaEntity.setLastEventId(contextDto.sourceEvent().eventId());
    orderSagaEntity.setErrorMessage(errorMessage);
    orderSagaEntity.setUpdatedAt(updatedAt);
  }

  private SagaContextDto loadSagaContext(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    OrderEntity orderEntity = this.orderRepository.findById(orderId).orElseThrow();
    OrderSagaEntity orderSagaEntity = this.sagaRepository.findByOrderId(orderId).orElseThrow();

    return SagaContextDto.builder()
        .orderEntity(orderEntity)
        .orderSagaEntity(orderSagaEntity)
        .sourceEvent(eventEnvelopeDto)
        .build();
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = this.processedEventMapper.toEntity(eventEnvelopeDto);

    this.processedEventRepository.save(processedEventEntity);
  }

}