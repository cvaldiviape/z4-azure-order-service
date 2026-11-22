package com.z4greed.order.controller;

import com.z4greed.shared.core.dto.PageResponseDto;
import com.z4greed.order.dto.PurchaseSagaDetailDto;
import com.z4greed.order.dto.PurchaseSagaSummaryDto;
import com.z4greed.shared.core.dto.ResponseDto;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.saga.PurchaseSagaQueryService;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

// API de lectura de Sagas para la Ops Console. Requiere rol ADMIN (ver SecurityConfig).
@RestController
@RequestMapping("/ops/purchase-sagas")
public class OpsPurchaseSagaController {
    private final PurchaseSagaQueryService purchaseSagaQueryService;

    public OpsPurchaseSagaController(PurchaseSagaQueryService purchaseSagaQueryService) {
        this.purchaseSagaQueryService = purchaseSagaQueryService;
    }

    // Ejemplo: GET /ops/purchase-sagas?status=TIMED_OUT&page=0&size=20
    @GetMapping
    public ResponseDto<PageResponseDto<PurchaseSagaSummaryDto>> search(
            @RequestParam(required = false) SagaStatusEnum status,
            @PageableDefault(size = 20, sort = "createdAt", direction = Sort.Direction.DESC) Pageable pageable) {
        return ResponseDto.<PageResponseDto<PurchaseSagaSummaryDto>>builder()
                .code("PURCHASE_SAGAS_FOUND")
                .statusCode(HttpStatus.OK.value())
                .message("Purchase sagas retrieved successfully")
                .data(this.purchaseSagaQueryService.search(status, pageable))
                .build();
    }

    @GetMapping("/{orderId}")
    public ResponseDto<PurchaseSagaDetailDto> getByOrderId(@PathVariable Long orderId) {
        return ResponseDto.<PurchaseSagaDetailDto>builder()
                .code("PURCHASE_SAGA_FOUND")
                .statusCode(HttpStatus.OK.value())
                .message("Purchase saga retrieved successfully")
                .data(this.purchaseSagaQueryService.getByOrderId(orderId))
                .build();
    }
}
