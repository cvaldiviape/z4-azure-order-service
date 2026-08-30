package com.z4greed.order.enums;

import java.util.Arrays;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EventTypeEnum {
  ORDER_CREATED("ORDER_CREATED"),
  STOCK_RESERVED("STOCK_RESERVED"),
  STOCK_NOT_AVAILABLE("STOCK_NOT_AVAILABLE"),
  STOCK_RELEASED("STOCK_RELEASED"),
  PAYMENT_REQUESTED("PAYMENT_REQUESTED"),
  PAYMENT_APPROVED("PAYMENT_APPROVED"),
  PAYMENT_FAILED("PAYMENT_FAILED"),
  RELEASE_STOCK("RELEASE_STOCK"),
  ORDER_CANCELLED("ORDER_CANCELLED");

  private final String value;

  public static Optional<EventTypeEnum> fromValue(String value) {
    EventTypeEnum[] listEnumValues = values();

    return Arrays.stream(listEnumValues)
            .filter(eventTypeEnum -> {
              String valueEvent = eventTypeEnum.value;
              return valueEvent.equals(value);
            })
            .findFirst();
  }

}
