package com.z4greed.order.service.impl;

import com.z4greed.order.dto.*;
import com.z4greed.order.entity.*;
import com.z4greed.order.enums.*;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.mapper.OrderMapper;
import com.z4greed.order.repository.*;
import com.z4greed.order.service.OrderService;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import jakarta.validation.Valid;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class OrderServiceImpl implements OrderService {
  private final OrderRepository orderRepository;
  private final SagaRepository sagaRepository;
  private final OrderEventProducer orderEventProducer;
  private final OrderMapper orderMapper;
  private final OrderEventFactory orderEventFactory;

  public OrderServiceImpl(
      OrderRepository orderRepository,
      SagaRepository sagaRepository,
      OrderEventProducer orderEventProducer,
      OrderMapper orderMapper,
      OrderEventFactory orderEventFactory
  ) {
    this.orderRepository = orderRepository;
    this.sagaRepository = sagaRepository;
    this.orderEventProducer = orderEventProducer;
    this.orderMapper = orderMapper;
    this.orderEventFactory = orderEventFactory;
  }

  @Override
  public OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto) {
    OrderEntity orderEntity = this.createOrder(customerId, requestDto);
    this.createSaga(orderEntity);
    this.publishOrderCreated(orderEntity, requestDto);
    return this.orderMapper.toDto(orderEntity);
  }

  private OrderEntity createOrder(Long customerId, CreateOrderRequestDto requestDto) {
    List<@Valid ItemRequestDto> listItems = requestDto.listItems();

    BigDecimal totalAmount = this.calculateTotal(listItems);
    OrderCreateDto orderCreateDto = this.buildOrderCreate(customerId, requestDto, totalAmount);

    OrderEntity orderEntity = this.orderMapper.toEntity(orderCreateDto);
    List<OrderItemEntity> listItemEntities = this.buildListItemsEntities(listItems, orderEntity);

    orderEntity.setListItems(listItemEntities);

    return this.orderRepository.save(orderEntity);
  }

  private BigDecimal calculateTotal(List<ItemRequestDto> listItems) {
    return listItems.stream()
            .map(this::calculateSubtotal)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
  }

  private OrderCreateDto buildOrderCreate(Long customerId, CreateOrderRequestDto requestDto, BigDecimal totalAmount) {
      return OrderCreateDto.builder()
          .customerId(customerId)
          .status(OrderStatusEnum.CREATED)
          .totalAmount(totalAmount)
          .currency("PEN")
          .correlationId("purchase-" + UUID.randomUUID())
          .paymentToken(requestDto.paymentToken())
          .createdAt(LocalDateTime.now())
          .build();
  }

  private List<OrderItemEntity> buildListItemsEntities(List<ItemRequestDto> listItemDtos, OrderEntity orderEntity) {
    return listItemDtos.stream()
        .map(itemDto -> {
          BigDecimal subtotal = this.calculateSubtotal(itemDto);

          OrderItemCreateDto orderItemCreateDto = OrderItemCreateDto.builder()
                  .order(orderEntity)
                  .productId(itemDto.productId())
                  .productName(itemDto.productName())
                  .unitPrice(itemDto.unitPrice())
                  .quantity(itemDto.quantity())
                  .subtotal(subtotal)
                  .build();

          return this.orderMapper.toItemEntity(orderItemCreateDto);
        })
        .toList();
  }

  private BigDecimal calculateSubtotal(ItemRequestDto item) {
    BigDecimal unitPrice = item.unitPrice();
    Integer quantity = item.quantity();

    return unitPrice.multiply(BigDecimal.valueOf(quantity));
  }

  private void createSaga(OrderEntity orderEntity) {
    OrderSagaCreateDto orderSagaCreateDto = OrderSagaCreateDto.builder()
        .order(orderEntity)
        .status(SagaStatusEnum.STARTED)
        .currentStep(EventTypeEnum.ORDER_CREATED.getValue())
        .createdAt(LocalDateTime.now())
        .build();

    OrderSagaEntity orderSagaEntity = this.orderMapper.toSagaEntity(orderSagaCreateDto);

    this.sagaRepository.save(orderSagaEntity);
  }

  private void publishOrderCreated(OrderEntity orderEntity, CreateOrderRequestDto requestDto) {
    Map<String, Object> mapPayload = this.buildMapPayload(orderEntity, requestDto);
    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.ORDER_CREATED, orderEntity,null, mapPayload);

    this.orderEventProducer.publish("orders.events", eventEnvelopeDto);
  }

  private Map<String, Object> buildMapPayload(OrderEntity orderEntity, CreateOrderRequestDto requestDto) {
    List<@Valid ItemRequestDto> listItems = requestDto.listItems();

      return Map.of(
          "customerId", orderEntity.getCustomerId(),
          "amount", orderEntity.getTotalAmount(),
          "currency", orderEntity.getCurrency(),
          "paymentToken", orderEntity.getPaymentToken(),
          "items", listItems
      );
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
    Long customerIdCurrent = orderEntity.getCustomerId();
    if (!customerIdCurrent.equals(customerId)) {
      throw new GreedException(ErrorCodeEnum.ORDER_ACCESS_DENIED);
    }
  }

}