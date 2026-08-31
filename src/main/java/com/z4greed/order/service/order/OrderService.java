package com.z4greed.order.service.order;

import com.z4greed.order.dto.CreateOrderRequestDto;
import com.z4greed.order.dto.OrderResponseDto;

public interface OrderService {
  OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto);
  OrderResponseDto get(Long orderId, Long customerId);
}
