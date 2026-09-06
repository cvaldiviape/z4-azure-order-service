package com.z4greed.order.exception;

import com.z4greed.order.enums.ErrorCodeEnum;
import lombok.Getter;

@Getter
public class CustomBusinessException extends RuntimeException {
  private final ErrorCodeEnum errorCode;

  public CustomBusinessException(ErrorCodeEnum errorCode) {
    super(errorCode.getMessage());
    this.errorCode = errorCode;
  }
}
