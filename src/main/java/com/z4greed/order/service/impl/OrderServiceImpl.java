package com.z4greed.order.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.order.dto.*;
import com.z4greed.order.entity.*;
import com.z4greed.order.enums.*;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.mapper.OrderMapper;
import com.z4greed.order.repository.*;
import com.z4greed.order.service.OrderService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class OrderServiceImpl implements OrderService {
  private final OrderRepository orderRepository;
  private final SagaRepository sagaRepository;
  private final OrderEventProducer orderEventProducer;
  private final OrderMapper orderMapper;
  private final ObjectMapper objectMapper;

  public OrderServiceImpl(
      OrderRepository orderRepository,
      SagaRepository sagaRepository,
      OrderEventProducer orderEventProducer,
      OrderMapper orderMapper,
      ObjectMapper objectMapper) {
    this.orderRepository = orderRepository;
    this.sagaRepository = sagaRepository;
    this.orderEventProducer = orderEventProducer;
    this.orderMapper = orderMapper;
    this.objectMapper = objectMapper;
  }

  @Override
  public OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto) {
    OrderEntity orderEntity = this.createOrder(customerId, requestDto);
    this.createSaga(orderEntity);
    this.publishOrderCreated(orderEntity, requestDto.listItems());
    return this.orderMapper.toDto(orderEntity);
  }

  private OrderEntity createOrder(Long customerId, CreateOrderRequestDto requestDto) {
    BigDecimal totalAmount = this.calculateTotal(requestDto.listItems());
    OrderCreateDto orderCreateDto = OrderCreateDto.builder()
        .customerId(customerId)
        .status(OrderStatusEnum.CREATED)
        .totalAmount(totalAmount)
        .currency("PEN")
        .correlationId("purchase-" + UUID.randomUUID())
        .paymentToken(requestDto.paymentToken())
        .createdAt(Instant.now())
        .build();
    OrderEntity orderEntity = this.orderMapper.toEntity(orderCreateDto);
    List<OrderItemEntity> listItemEntities = this.createItems(requestDto.listItems(), orderEntity);
    orderEntity.setListItems(listItemEntities);
    return this.orderRepository.save(orderEntity);
  }

  private BigDecimal calculateTotal(List<ItemRequestDto> listItemDtos) {
    return listItemDtos.stream()
        .map(itemDto -> this.calculateSubtotal(itemDto))
        .reduce(BigDecimal.ZERO, BigDecimal::add);
  }

  private BigDecimal calculateSubtotal(ItemRequestDto itemDto) {
    return itemDto.unitPrice().multiply(BigDecimal.valueOf(itemDto.quantity()));
  }

  private List<OrderItemEntity> createItems(List<ItemRequestDto> listItemDtos, OrderEntity orderEntity) {
    return listItemDtos.stream()
        .map(itemDto -> this.createItem(itemDto, orderEntity))
        .toList();
  }

  private OrderItemEntity createItem(ItemRequestDto itemDto, OrderEntity orderEntity) {
    OrderItemCreateDto orderItemCreateDto = OrderItemCreateDto.builder()
        .order(orderEntity)
        .productId(itemDto.productId())
        .productName(itemDto.productName())
        .unitPrice(itemDto.unitPrice())
        .quantity(itemDto.quantity())
        .subtotal(this.calculateSubtotal(itemDto))
        .build();
    return this.orderMapper.toItemEntity(orderItemCreateDto);
  }

  private void createSaga(OrderEntity orderEntity) {
    OrderSagaCreateDto orderSagaCreateDto = OrderSagaCreateDto.builder()
        .order(orderEntity)
        .status(SagaStatusEnum.STARTED)
        .currentStep(EventTypeEnum.ORDER_CREATED.getValue())
        .createdAt(Instant.now())
        .build();
    OrderSagaEntity orderSagaEntity = this.orderMapper.toSagaEntity(orderSagaCreateDto);
    this.sagaRepository.save(orderSagaEntity);
  }

  private void publishOrderCreated(OrderEntity orderEntity, List<ItemRequestDto> listItemDtos) {
    Map<String, Object> mapPayload = Map.of(
        "customerId", orderEntity.getCustomerId(),
        "amount", orderEntity.getTotalAmount(),
        "currency", orderEntity.getCurrency(),
        "paymentToken", orderEntity.getPaymentToken(),
        "items", listItemDtos);
    String eventType = EventTypeEnum.ORDER_CREATED.getValue();
    EventEnvelopeDto eventEnvelopeDto = this.createEvent(eventType, orderEntity, null, mapPayload);
    this.orderEventProducer.publish("orders.events", eventEnvelopeDto);
  }

  @Override
  public EventEnvelopeDto createEvent(String eventType, OrderEntity orderEntity, String causationId, Object payload) {
    return EventEnvelopeDto.builder()
        .eventId(UUID.randomUUID().toString())
        .eventType(eventType)
        .eventVersion(1)
        .aggregateId(orderEntity.getId().toString())
        .correlationId(orderEntity.getCorrelationId())
        .causationId(causationId)
        .timestamp(Instant.now())
        .producer("order-service")
        .payload(this.objectMapper.valueToTree(payload))
        .build();
  }

  @Override
  @Transactional(readOnly = true)
  public OrderResponseDto get(Long orderId, Long customerId) {
    OrderEntity orderEntity = this.findOrder(orderId);
    this.validateOwnership(orderEntity, customerId);
    return this.orderMapper.toDto(orderEntity);
  }

  private OrderEntity findOrder(Long orderId) {
    return this.orderRepository.findById(orderId)
        .orElseThrow(() -> new GreedException(ErrorCodeEnum.ORDER_NOT_FOUND));
  }

  private void validateOwnership(OrderEntity orderEntity, Long customerId) {
    if (!orderEntity.getCustomerId().equals(customerId)) {
      throw new GreedException(ErrorCodeEnum.ORDER_ACCESS_DENIED);
    }
  }

}