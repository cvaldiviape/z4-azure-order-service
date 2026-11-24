package com.z4greed.order.service.impl;

import com.z4greed.order.dto.CreateOrderRequestDto;
import com.z4greed.order.dto.ItemRequestDto;
import com.z4greed.order.dto.OrderResponseDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.OrderItemEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.entity.PurchaseSagaHistoryEntity;
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
import com.z4greed.order.repository.PurchaseSagaHistoryRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
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
    private final PurchaseSagaHistoryRepository purchaseSagaHistoryRepository;
    private final OutboxEventService outboxEventService;
    private final OrderMapper orderMapper;
    private final OrderEventFactory orderEventFactory;

    public OrderServiceImpl(
            OrderRepository orderRepository,
            PurchaseSagaRepository purchaseSagaRepository,
            PurchaseSagaHistoryRepository purchaseSagaHistoryRepository,
            OutboxEventService outboxEventService,
            OrderMapper orderMapper,
            OrderEventFactory orderEventFactory
    ) {
        this.orderRepository = orderRepository;
        this.purchaseSagaRepository = purchaseSagaRepository;
        this.purchaseSagaHistoryRepository = purchaseSagaHistoryRepository;
        this.outboxEventService = outboxEventService;
        this.orderMapper = orderMapper;
        this.orderEventFactory = orderEventFactory;
    }

    @Override
    public OrderResponseDto create(Long customerId, CreateOrderRequestDto requestDto) {
        OrderEntity orderEntity = this.createOrder(customerId, requestDto);
        IntegrationEventDto orderCreatedEvent = this.orderEventFactory.build(EventTypeEnum.ORDER_CREATED, orderEntity);

        this.startPurchaseSaga(orderEntity, orderCreatedEvent);
        this.requestStockReservation(orderEntity, orderCreatedEvent, requestDto.listItems());

        return this.orderMapper.toDto(orderEntity);
    }

    private OrderEntity createOrder(Long customerId, CreateOrderRequestDto requestDto) {
        List<ItemRequestDto> listItems = requestDto.listItems();

        BigDecimal totalAmount = this.calculateTotal(listItems);
        OrderEntity orderEntity = this.buildOrderEntity(customerId, requestDto, totalAmount);
        List<OrderItemEntity> listItemsEntites = this.buildOrderItems(listItems, orderEntity);

        orderEntity.setListItems(listItemsEntites);

        return this.orderRepository.save(orderEntity);
    }

    private BigDecimal calculateTotal(List<ItemRequestDto> listItems) {
        return listItems.stream()
                .map(this::calculateSubtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private OrderEntity buildOrderEntity(Long customerId, CreateOrderRequestDto requestDto, BigDecimal totalAmount) {
        return OrderEntity.builder()
                .customerId(customerId)
                .status(OrderStatusEnum.CREATED)
                .totalAmount(totalAmount)
                .currency(CURRENCY)
                .correlationId(CORRELATION_ID_PREFIX + UUID.randomUUID())
                .paymentToken(requestDto.paymentToken())
                .createdAt(LocalDateTime.now())
                .build();
    }

    private List<OrderItemEntity> buildOrderItems(List<ItemRequestDto> listItems, OrderEntity orderEntity) {
        return listItems.stream()
                .map(itemDto -> {
                    BigDecimal subtotal = this.calculateSubtotal(itemDto);
                    return OrderItemEntity.builder()
                            .order(orderEntity)
                            .productId(itemDto.productId())
                            .productName(itemDto.productName())
                            .unitPrice(itemDto.unitPrice())
                            .quantity(itemDto.quantity())
                            .subtotal(subtotal)
                            .build();
                })
                .toList();
    }

    private BigDecimal calculateSubtotal(ItemRequestDto item) {
        return item.unitPrice().multiply(BigDecimal.valueOf(item.quantity()));
    }

    private void startPurchaseSaga(OrderEntity orderEntity, IntegrationEventDto orderCreatedEvent) {
        LocalDateTime createdAt = LocalDateTime.now();
        String orderCreatedEventId = orderCreatedEvent.eventId();

        PurchaseSagaEntity purchaseSagaEntity = this.createPurchaseSaga(orderEntity, orderCreatedEventId, createdAt);
        this.createPurchaseSagaHistory(purchaseSagaEntity, orderCreatedEventId, createdAt);
    }

    private PurchaseSagaEntity createPurchaseSaga(OrderEntity orderEntity, String orderCreatedEventId, LocalDateTime createdAt) {
        PurchaseSagaEntity purchaseSagaEntity = PurchaseSagaEntity.builder()
                .order(orderEntity)
                .status(SagaStatusEnum.STARTED)
                .currentStep(EventTypeEnum.ORDER_CREATED.getValue())
                .lastEventId(orderCreatedEventId)
                .createdAt(createdAt)
                .build();

        return this.purchaseSagaRepository.save(purchaseSagaEntity);
    }

    private void createPurchaseSagaHistory(PurchaseSagaEntity purchaseSagaEntity, String orderCreatedEventId, LocalDateTime createdAt) {
        OrderEntity orderEntity = purchaseSagaEntity.getOrder();

        PurchaseSagaHistoryEntity purchaseSagaHistoryEntity = PurchaseSagaHistoryEntity.builder()
                .purchaseSaga(purchaseSagaEntity)
                .orderId(orderEntity.getId())
                .orderStatus(orderEntity.getStatus())
                .sagaStatus(purchaseSagaEntity.getStatus())
                .eventType(EventTypeEnum.ORDER_CREATED)
                .eventId(orderCreatedEventId)
                .errorMessage(purchaseSagaEntity.getErrorMessage())
                .createdAt(createdAt)
                .build();

        this.purchaseSagaHistoryRepository.save(purchaseSagaHistoryEntity);
    }

    private void requestStockReservation(OrderEntity orderEntity, IntegrationEventDto orderCreatedEvent, List<ItemRequestDto> listItems) {
        Map<String, Object> mapPayload = Map.of(
                "customerId", orderEntity.getCustomerId(),
                "amount", orderEntity.getTotalAmount(),
                "currency", orderEntity.getCurrency(),
                "paymentToken", orderEntity.getPaymentToken(),
                "items", listItems
        );
        String previousEventId = orderCreatedEvent.eventId();
        IntegrationEventDto reserveStockEvent = this.orderEventFactory.build(EventTypeEnum.RESERVE_STOCK, orderEntity, previousEventId, mapPayload);

        this.outboxEventService.enqueue(KafkaTopics.INVENTORY_RESERVATION_REQUESTS, reserveStockEvent);
    }

    @Override
    @Transactional(readOnly = true)
    public OrderResponseDto get(Long orderId, Long customerId) {
        OrderEntity orderEntity = this.findOrder(orderId);
        this.validateOwnership(orderEntity, customerId);
        return this.orderMapper.toDto(orderEntity);
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