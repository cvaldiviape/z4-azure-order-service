package com.z4greed.order.exception;

import com.z4greed.order.enums.ErrorCodeEnum;
import lombok.Getter;

@Getter
public class GreedException extends RuntimeException {
  private final ErrorCodeEnum errorCode;

  public GreedException(ErrorCodeEnum errorCode) {
    super(errorCode.getMessage());
    this.errorCode = errorCode;
  }

  public GreedException(ErrorCodeEnum errorCode, Throwable cause) {
    super(errorCode.getMessage(), cause);
    this.errorCode = errorCode;
  }

}