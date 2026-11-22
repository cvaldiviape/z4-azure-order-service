package com.z4greed.order.service.impl;

import com.z4greed.order.dto.CreateOrderRequestDto;
import com.z4greed.order.dto.ItemRequestDto;
import com.z4greed.order.dto.OrderResponseDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.OrderItemEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.shared.core.exception.BusinessException;
import com.z4greed.order.kafka.KafkaTopics;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.order.mapper.OrderMapper;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.saga.state.PurchaseSagaHistoryRecorder;
import com.z4greed.order.service.OrderService;
import com.z4greed.shared.kafka.outbox.OutboxEventService;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class OrderServiceImpl implements OrderService {
    private static final String CURRENCY = "PEN";
    private static final String CORRELATION_ID_PREFIX = "purchase-";

    private final OrderRepository orderRepository;
    private final PurchaseSagaRepository purchaseSagaRepository;
    private final PurchaseSagaHistoryRecorder purchaseSagaHistoryRecorder;
    private final OutboxEventService outboxEventService;
    private final OrderMapper orderMapper;
    private final OrderEventFactory orderEventFactory;

    public OrderServiceImpl(
            OrderRepository orderRepository,
            PurchaseSagaRepository purchaseSagaRepository,
            PurchaseSagaHistoryRecorder purchaseSagaHistoryRecorder,
            OutboxEventService outboxEventService,
            OrderMapper orderMapper,
            OrderEventFactory orderEventFactory
    ) {
        this.orderRepository = orderRepository;
        this.purchaseSagaRepository = purchaseSagaRepository;
        this.purchaseSagaHistoryRecorder = purchaseSagaHistoryRecorder;
        this.outboxEventService = outboxEventService;
        this.orderMapper = orderMapper;
        this.orderEventFactory = orderEventFactory;
    }

    // Crear un pedido = guardarlo, iniciar su Saga y pedir a inventory-service que reserve el stock.
    @Override
    public OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto) {
        OrderEntity orderEntity = this.createOrder(customerId, requestDto);
        IntegrationEventDto orderCreatedEvent = this.orderEventFactory.build(EventTypeEnum.ORDER_CREATED, orderEntity, null, Map.of());

        this.startPurchaseSaga(orderEntity, orderCreatedEvent);
        this.requestStockReservation(orderEntity, orderCreatedEvent, requestDto.listItems());

        return this.orderMapper.toDto(orderEntity);
    }

    @Override
    @Transactional(readOnly = true)
    public OrderResponseDto get(Long orderId, Long customerId) {
        OrderEntity orderEntity = this.findOrder(orderId);
        this.validateOwnership(orderEntity, customerId);
        return this.orderMapper.toDto(orderEntity);
    }

    private OrderEntity createOrder(Long customerId, CreateOrderRequestDto requestDto) {
        List<ItemRequestDto> listItems = requestDto.listItems();

        OrderEntity orderEntity = OrderEntity.builder()
                .customerId(customerId)
                .status(OrderStatusEnum.CREATED)
                .totalAmount(this.calculateTotal(listItems))
                .currency(CURRENCY)
                .correlationId(CORRELATION_ID_PREFIX + UUID.randomUUID())
                .paymentToken(requestDto.paymentToken())
                .createdAt(LocalDateTime.now())
                .build();
        orderEntity.setListItems(this.buildOrderItems(listItems, orderEntity));

        return this.orderRepository.save(orderEntity);
    }

    private BigDecimal calculateTotal(List<ItemRequestDto> listItems) {
        return listItems.stream()
                .map(this::calculateSubtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private List<OrderItemEntity> buildOrderItems(List<ItemRequestDto> listItems, OrderEntity orderEntity) {
        return listItems.stream()
                .map(itemDto -> OrderItemEntity.builder()
                        .order(orderEntity)
                        .productId(itemDto.productId())
                        .productName(itemDto.productName())
                        .unitPrice(itemDto.unitPrice())
                        .quantity(itemDto.quantity())
                        .subtotal(this.calculateSubtotal(itemDto))
                        .build())
                .toList();
    }

    private BigDecimal calculateSubtotal(ItemRequestDto item) {
        return item.unitPrice().multiply(BigDecimal.valueOf(item.quantity()));
    }

    private void startPurchaseSaga(OrderEntity orderEntity, IntegrationEventDto orderCreatedEvent) {
        LocalDateTime createdAt = LocalDateTime.now();

        PurchaseSagaEntity purchaseSagaEntity = this.purchaseSagaRepository.save(PurchaseSagaEntity.builder()
                .order(orderEntity)
                .status(SagaStatusEnum.STARTED)
                .currentStep(EventTypeEnum.ORDER_CREATED.getValue())
                .lastEventId(orderCreatedEvent.eventId())
                .createdAt(createdAt)
                .build());

        this.purchaseSagaHistoryRecorder.record(purchaseSagaEntity, EventTypeEnum.ORDER_CREATED, orderCreatedEvent.eventId(), createdAt);
    }

    private void requestStockReservation(OrderEntity orderEntity, IntegrationEventDto orderCreatedEvent, List<ItemRequestDto> listItems) {
        Map<String, Object> mapPayload = Map.of(
                "customerId", orderEntity.getCustomerId(),
                "amount", orderEntity.getTotalAmount(),
                "currency", orderEntity.getCurrency(),
                "paymentToken", orderEntity.getPaymentToken(),
                "items", listItems
        );
        IntegrationEventDto reserveStockEvent = this.orderEventFactory.build(EventTypeEnum.RESERVE_STOCK, orderEntity, orderCreatedEvent.eventId(), mapPayload);

        this.outboxEventService.enqueue(KafkaTopics.INVENTORY_RESERVATION_REQUESTS, reserveStockEvent);
    }

    private OrderEntity findOrder(Long orderId) {
        return this.orderRepository.findById(orderId)
                .orElseThrow(() -> new BusinessException(ErrorCodeEnum.ORDER_NOT_FOUND));
    }

    private void validateOwnership(OrderEntity orderEntity, Long customerId) {
        if (!orderEntity.getCustomerId().equals(customerId)) {
            throw new BusinessException(ErrorCodeEnum.ORDER_ACCESS_DENIED);
        }
    }

}
