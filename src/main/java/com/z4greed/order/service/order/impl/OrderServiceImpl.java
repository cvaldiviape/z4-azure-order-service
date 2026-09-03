package com.z4greed.order.service.order.impl;

import com.z4greed.order.dto.*;
import com.z4greed.order.entity.*;
import com.z4greed.order.enums.*;
import com.z4greed.order.exception.CustomBusinessException;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.mapper.OrderMapper;
import com.z4greed.order.repository.*;
import com.z4greed.order.service.order.OrderService;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
@Slf4j
public class OrderServiceImpl implements OrderService {
  private final OrderRepository orderRepository;
  private final PurchaseSagaRepository purchaseSagaRepository;
  private final OrderEventProducer orderEventProducer;
  private final OrderMapper orderMapper;
  private final OrderEventFactory orderEventFactory;
  private final PurchaseSagaHistoryRepository purchaseSagaHistoryRepository;

  public OrderServiceImpl(
      OrderRepository orderRepository,
      PurchaseSagaRepository purchaseSagaRepository,
      OrderEventProducer orderEventProducer,
      OrderMapper orderMapper,
      OrderEventFactory orderEventFactory,
      PurchaseSagaHistoryRepository purchaseSagaHistoryRepository
  ) {
    this.orderRepository = orderRepository;
    this.purchaseSagaRepository = purchaseSagaRepository;
    this.orderEventProducer = orderEventProducer;
    this.orderMapper = orderMapper;
    this.orderEventFactory = orderEventFactory;
    this.purchaseSagaHistoryRepository = purchaseSagaHistoryRepository;
  }

  @Override
  public OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto) {
    OrderEntity orderEntity = this.createOrder(customerId, requestDto);
    EventEnvelopeDto orderCreatedEvent = this.buildOrderCreatedEvent(orderEntity);

    LocalDateTime createdAt = LocalDateTime.now();
    PurchaseSagaEntity savedPurchaseSagaEntity = this.createPurchaseSaga(orderEntity, orderCreatedEvent, createdAt);
    this.createPurchaseSagaHistory(orderEntity, orderCreatedEvent, savedPurchaseSagaEntity, createdAt);
    log.info("action=saga_started eventType={} eventId={} correlationId={} orderId={} orderStatus={} sagaStatus={}", orderCreatedEvent.eventType(), orderCreatedEvent.eventId(), orderCreatedEvent.correlationId(), orderEntity.getId(), orderEntity.getStatus(), savedPurchaseSagaEntity.getStatus());

    EventEnvelopeDto reserveStockCommand = this.buildReserveStockCommand(orderEntity, requestDto, orderCreatedEvent.eventId());
    this.publishReserveStock(reserveStockCommand);
    return this.orderMapper.toDto(orderEntity);
  }

  private OrderEntity createOrder(Long customerId, CreateOrderRequestDto requestDto) {
    List<@Valid ItemRequestDto> listItems = requestDto.listItems();

    BigDecimal totalAmount = this.calculateTotal(listItems);
    OrderEntity orderEntity = this.buildOrderEntity(customerId, requestDto, totalAmount);
    List<OrderItemEntity> listItemEntities = this.buildListItemsEntities(listItems, orderEntity);

    orderEntity.setListItems(listItemEntities);

    return this.orderRepository.save(orderEntity);
  }

  private BigDecimal calculateTotal(List<ItemRequestDto> listItems) {
    return listItems.stream()
            .map(this::calculateSubtotal)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
  }

  private OrderEntity buildOrderEntity(Long customerId, CreateOrderRequestDto requestDto, BigDecimal totalAmount) {
      return OrderEntity.builder()
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

          return OrderItemEntity.builder()
                  .order(orderEntity)
                  .productId(itemDto.productId())
                  .productName(itemDto.productName())
                  .unitPrice(itemDto.unitPrice())
                  .quantity(itemDto.quantity())
                  .subtotal(subtotal)
                  .build();
        })
        .toList();
  }

  private BigDecimal calculateSubtotal(ItemRequestDto item) {
    BigDecimal unitPrice = item.unitPrice();
    Integer quantity = item.quantity();

    return unitPrice.multiply(BigDecimal.valueOf(quantity));
  }

  private PurchaseSagaEntity createPurchaseSaga(OrderEntity orderEntity, EventEnvelopeDto eventEnvelopeDto, LocalDateTime createdAt) {
    PurchaseSagaEntity purchaseSagaEntity = PurchaseSagaEntity.builder()
        .order(orderEntity)
        .status(SagaStatusEnum.STARTED)
        .currentStep(EventTypeEnum.ORDER_CREATED.getValue())
        .lastEventId(eventEnvelopeDto.eventId())
        .createdAt(createdAt)
        .build();

      return this.purchaseSagaRepository.save(purchaseSagaEntity);
  }

  private void createPurchaseSagaHistory(OrderEntity orderEntity, EventEnvelopeDto eventEnvelopeDto, PurchaseSagaEntity savedPurchaseSagaEntity, LocalDateTime createdAt) {
    PurchaseSagaHistoryEntity purchaseSagaHistoryEntity = PurchaseSagaHistoryEntity.builder()
        .purchaseSaga(savedPurchaseSagaEntity)
        .orderId(orderEntity.getId())
        .orderStatus(orderEntity.getStatus())
        .sagaStatus(savedPurchaseSagaEntity.getStatus())
        .eventType(EventTypeEnum.ORDER_CREATED)
        .eventId(eventEnvelopeDto.eventId())
        .errorMessage(savedPurchaseSagaEntity.getErrorMessage())
        .createdAt(createdAt)
        .build();

    this.purchaseSagaHistoryRepository.save(purchaseSagaHistoryEntity);
  }

  private EventEnvelopeDto buildOrderCreatedEvent(OrderEntity orderEntity) {
    Map<String, Object> mapPayload = Map.of();
    return this.orderEventFactory.build(EventTypeEnum.ORDER_CREATED, orderEntity, null, mapPayload);
  }

  private EventEnvelopeDto buildReserveStockCommand(OrderEntity orderEntity, CreateOrderRequestDto requestDto, String causationId) {
    Map<String, Object> mapPayload = this.buildMapPayload(orderEntity, requestDto);
    return this.orderEventFactory.build(EventTypeEnum.RESERVE_STOCK, orderEntity, causationId, mapPayload);
  }

  private void publishReserveStock(EventEnvelopeDto eventEnvelopeDto) {
    this.orderEventProducer.publish("inventory-commands-topic", eventEnvelopeDto);
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
        .orElseThrow(() -> new CustomBusinessException(ErrorCodeEnum.ORDER_NOT_FOUND));
  }

  private void validateOwnership(OrderEntity orderEntity, Long customerId) {
    Long customerIdCurrent = orderEntity.getCustomerId();
    if (!customerIdCurrent.equals(customerId)) {
      throw new CustomBusinessException(ErrorCodeEnum.ORDER_ACCESS_DENIED);
    }
  }

}
