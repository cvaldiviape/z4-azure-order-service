package com.z4greed.order.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum {
  ORDER_NOT_FOUND(HttpStatus.NOT_FOUND, "Order not found"),
  ORDER_ACCESS_DENIED(HttpStatus.FORBIDDEN, "Order access denied"),
  INVALID_EVENT(HttpStatus.BAD_REQUEST, "Invalid event"),
  EVENT_PUBLISH_FAILED(HttpStatus.INTERNAL_SERVER_ERROR, "Event publish failed");
  private final HttpStatus httpStatus;
  private final String message;
}
