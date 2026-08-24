package com.z4greed.order.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;
import lombok.Builder;

@Builder
public record CreateOrderRequestDto(
    @NotEmpty List<@Valid ItemRequestDto> listItems, @NotBlank String paymentToken) {}
