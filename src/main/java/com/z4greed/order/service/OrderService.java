package com.z4greed.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.order.dto.*;
import com.z4greed.order.entity.*;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.SagaRepository;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class OrderService {
  private final OrderRepository orderRepository;
  private final SagaRepository sagaRepository;
  private final OrderEventProducer orderEventProducer;
  private final ObjectMapper objectMapper;

  public OrderService(
      OrderRepository orderRepository,
      SagaRepository sagaRepository,
      OrderEventProducer orderEventProducer,
      ObjectMapper objectMapper) {
    this.orderRepository = orderRepository;
    this.sagaRepository = sagaRepository;
    this.orderEventProducer = orderEventProducer;
    this.objectMapper = objectMapper;
  }

  public OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto) {
    OrderEntity orderEntity = OrderEntity.create(customerId, requestDto.paymentToken());
    requestDto
        .listItems()
        .forEach(
            itemRequestDto -> {
              OrderItemEntity orderItemEntity =
                  OrderItemEntity.builder()
                      .orderEntity(orderEntity)
                      .productId(itemRequestDto.productId())
                      .productName(itemRequestDto.productName())
                      .unitPrice(itemRequestDto.unitPrice())
                      .quantity(itemRequestDto.quantity())
                      .build();
              orderEntity.addItem(orderItemEntity);
            });
    orderEntity.calculateTotal();
    this.orderRepository.save(orderEntity);

    OrderSagaEntity orderSagaEntity = OrderSagaEntity.builder().orderEntity(orderEntity).build();
    this.sagaRepository.save(orderSagaEntity);

    Map<String, Object> mapPayload =
        Map.of(
            "customerId", customerId,
            "amount", orderEntity.getTotalAmount(),
            "currency", orderEntity.getCurrency(),
            "paymentToken", orderEntity.getPaymentToken(),
            "items", requestDto.listItems());
    EventEnvelopeDto eventEnvelopeDto = this.createEvent(EventTypeEnum.ORDER_CREATED.getValue(), orderEntity, null, mapPayload);
    this.orderEventProducer.publish("orders.events", eventEnvelopeDto);
    return mapToResponseDto(orderEntity);
  }

  @Transactional(readOnly = true)
  public OrderResponseDto get(Long orderId, Long customerId) {
    OrderEntity orderEntity =
        orderRepository
            .findById(orderId)
        .orElseThrow(() -> new GreedException(ErrorCodeEnum.ORDER_NOT_FOUND));
    boolean belongsToAnotherCustomer = !orderEntity.getCustomerId().equals(customerId);
    if (belongsToAnotherCustomer) {
      throw new GreedException(ErrorCodeEnum.ORDER_ACCESS_DENIED);
    }
    return mapToResponseDto(orderEntity);
  }

  public EventEnvelopeDto createEvent(
      String eventType, OrderEntity orderEntity, String causationId, Object payload) {
    String eventId = UUID.randomUUID().toString();
    String aggregateId = orderEntity.getId().toString();
    return EventEnvelopeDto.builder()
        .eventId(eventId)
        .eventType(eventType)
        .eventVersion(1)
        .aggregateId(aggregateId)
        .correlationId(orderEntity.getCorrelationId())
        .causationId(causationId)
        .timestamp(Instant.now())
        .producer("order-service")
        .payload(objectMapper.valueToTree(payload))
        .build();
  }

  private OrderResponseDto mapToResponseDto(OrderEntity orderEntity) {
    var listItemResponseDtos =
        orderEntity.getListItems().stream()
            .map(
                orderItemEntity ->
                    ItemResponseDto.builder()
                        .productId(orderItemEntity.getProductId())
                        .productName(orderItemEntity.getProductName())
                        .unitPrice(orderItemEntity.getUnitPrice())
                        .quantity(orderItemEntity.getQuantity())
                        .subtotal(orderItemEntity.getSubtotal())
                        .build())
            .toList();
    return OrderResponseDto.builder()
        .id(orderEntity.getId())
        .customerId(orderEntity.getCustomerId())
        .status(orderEntity.getStatus().name())
        .totalAmount(orderEntity.getTotalAmount())
        .currency(orderEntity.getCurrency())
        .correlationId(orderEntity.getCorrelationId())
        .listItems(listItemResponseDtos)
        .build();
  }
}
