package com.z4greed.order.service;

import com.z4greed.order.dto.CreateOrderRequestDto;
import com.z4greed.order.dto.OrderResponseDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.kafka.event.EventEnvelopeDto;

public interface OrderService {
  OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto);
  OrderResponseDto get(Long orderId, Long customerId);
  EventEnvelopeDto buildEvent(String eventType, OrderEntity orderEntity, String causationId, Object payload);
}