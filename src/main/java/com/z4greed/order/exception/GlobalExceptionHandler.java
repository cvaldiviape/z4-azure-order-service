package com.z4greed.order.exception;

import com.z4greed.order.dto.ResponseDto;
import com.z4greed.order.enums.ErrorCodeEnum;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ResponseDto<Map<String, String>>> handleValidationException(MethodArgumentNotValidException exception) {
    ErrorCodeEnum errorCode = ErrorCodeEnum.VALIDATION_ERROR;
    Map<String, String> mapValidationErrors = this.getValidationErrors(exception);
    ResponseDto<Map<String, String>> responseDto = ResponseDto.<Map<String, String>>builder()
        .code(errorCode.name())
        .statusCode(errorCode.getHttpStatus().value())
        .message(errorCode.getMessage())
        .data(mapValidationErrors)
        .build();
    return ResponseEntity.status(errorCode.getHttpStatus()).body(responseDto);
  }

  private Map<String, String> getValidationErrors(MethodArgumentNotValidException exception) {
    Map<String, String> mapValidationErrors = new LinkedHashMap<>();
    List<FieldError> listFieldErrors = exception.getBindingResult().getFieldErrors();
    listFieldErrors.forEach(fieldError -> {
      String fieldName = fieldError.getField();
      String errorMessage = Objects.requireNonNullElse(fieldError.getDefaultMessage(), "Invalid value");
      mapValidationErrors.putIfAbsent(fieldName, errorMessage);
    });
    return mapValidationErrors;
  }

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
