package com.z4greed.order.enums;

import com.z4greed.shared.core.exception.ErrorCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

// VALIDATION_ERROR ya no está aquí: lo aporta CoreErrorCode de z4-shared-core.
@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum implements ErrorCode {
    ORDER_NOT_FOUND(HttpStatus.NOT_FOUND, "Order not found"),
    ORDER_ACCESS_DENIED(HttpStatus.FORBIDDEN, "Order access denied"),
    PURCHASE_SAGA_NOT_FOUND(HttpStatus.NOT_FOUND, "Purchase saga not found");
    private final HttpStatus httpStatus;
    private final String message;
}
