package com.z4greed.order.dto;

import lombok.Builder;

@Builder
public record ResponseDto<T>(
    String code,
    Integer statusCode,
    String message,
    T data) {}
