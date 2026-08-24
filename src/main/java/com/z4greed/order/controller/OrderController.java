package com.z4greed.order.controller;

import com.z4greed.order.dto.*;
import com.z4greed.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {
  private final OrderService orderService;

  public OrderController(
      OrderService orderService) {
    this.orderService = orderService;
  }

  @PostMapping
  @ResponseStatus(HttpStatus.CREATED)
  public ResponseDto<OrderResponseDto> create(
      @AuthenticationPrincipal Jwt jwt, @Valid @RequestBody CreateOrderRequestDto requestDto) {
    Long customerId = Long.valueOf(jwt.getSubject());
    OrderResponseDto orderResponseDto = this.orderService.create(customerId, requestDto);
    return ResponseDto.<OrderResponseDto>builder()
        .code("ORDER_CREATED")
        .statusCode(HttpStatus.CREATED.value())
        .message("Order created successfully")
        .data(orderResponseDto)
        .build();
  }

  @GetMapping("/{id}")
  public ResponseDto<OrderResponseDto> get(
      @AuthenticationPrincipal Jwt jwt, @PathVariable Long id) {
    Long customerId = Long.valueOf(jwt.getSubject());
    OrderResponseDto orderResponseDto = this.orderService.get(id, customerId);
    return ResponseDto.<OrderResponseDto>builder()
        .code("ORDER_FOUND")
        .statusCode(HttpStatus.OK.value())
        .message("Order retrieved successfully")
        .data(orderResponseDto)
        .build();
  }
}
