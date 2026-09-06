package com.z4greed.order.exception;

import com.z4greed.order.enums.ErrorCodeEnum;
import lombok.Getter;

@Getter
public class CustomNonRetryableKafkaException extends RuntimeException {
  private final ErrorCodeEnum errorCode;

  public CustomNonRetryableKafkaException(ErrorCodeEnum errorCode, Throwable cause) {
    super(errorCode.getMessage(), cause);
    this.errorCode = errorCode;
  }
}
