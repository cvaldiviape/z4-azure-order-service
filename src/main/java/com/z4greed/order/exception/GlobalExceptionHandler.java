package com.z4greed.order.exception;

import com.z4greed.order.dto.ResponseDto;
import com.z4greed.order.enums.ErrorCodeEnum;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(GreedException.class)
  public ResponseEntity<ResponseDto<Void>> handleGreedException(GreedException exception) {
    ErrorCodeEnum errorCode = exception.getErrorCode();
    ResponseDto<Void> responseDto =
        ResponseDto.<Void>builder()
            .code(errorCode.name())
            .statusCode(errorCode.getHttpStatus().value())
            .message(exception.getMessage())
            .build();
    return ResponseEntity.status(errorCode.getHttpStatus()).body(responseDto);
  }

}