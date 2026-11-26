package com.z4greed.order.saga.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.kafka.KafkaTopics;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.saga.PurchaseSagaService;
import com.z4greed.order.strategy.EventStrategy;
import com.z4greed.order.strategy.EventStrategyRegistry;
import com.z4greed.order.strategy.PaymentEventStrategyRegistry;
import com.z4greed.order.strategy.StockEventStrategyRegistry;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.event.IntegrationEventReader;
import com.z4greed.shared.kafka.exception.NonRetryableKafkaException;
import com.z4greed.shared.kafka.inbox.InboxEventService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class PurchaseSagaServiceImpl implements PurchaseSagaService {
    private final OrderRepository orderRepository;
    private final PurchaseSagaRepository purchaseSagaRepository;
    private final InboxEventService inboxEventService;
    private final StockEventStrategyRegistry stockEventStrategyRegistry;
    private final PaymentEventStrategyRegistry paymentEventStrategyRegistry;
    private final IntegrationEventReader integrationEventReader;

    public PurchaseSagaServiceImpl(
            OrderRepository orderRepository,
            PurchaseSagaRepository purchaseSagaRepository,
            InboxEventService inboxEventService,
            StockEventStrategyRegistry stockEventStrategyRegistry,
            PaymentEventStrategyRegistry paymentEventStrategyRegistry,
            IntegrationEventReader integrationEventReader
    ) {
        this.orderRepository = orderRepository;
        this.purchaseSagaRepository = purchaseSagaRepository;
        this.inboxEventService = inboxEventService;
        this.stockEventStrategyRegistry = stockEventStrategyRegistry;
        this.paymentEventStrategyRegistry = paymentEventStrategyRegistry;
        this.integrationEventReader = integrationEventReader;
    }

    @Override
    public void handleInventoryEvent(String rawEvent) {
        this.handle(rawEvent, KafkaTopics.INVENTORY_RESERVATION_RESULTS, this.stockEventStrategyRegistry);
    }

    @Override
    public void handlePaymentEvent(String rawEvent) {
        this.handle(rawEvent, KafkaTopics.PAYMENT_PROCESSING_RESULTS, this.paymentEventStrategyRegistry);
    }

    // Flujo común a todos los resultados que recibe la Saga: leer, descartar duplicados, ejecutar la estrategia y registrar en el inbox.
    private void handle(String rawEvent, String sourceTopic, EventStrategyRegistry<EventTypeEnum, PurchaseSagaContextDto> strategyRegistry) {
        IntegrationEventDto event = this.integrationEventReader.read(rawEvent);

        if (this.inboxEventService.wasAlreadyProcessed(event.eventId())) {
            return;
        }

        EventStrategy<EventTypeEnum, PurchaseSagaContextDto> eventStrategy = this.resolveStrategy(strategyRegistry, event.eventType(), sourceTopic);
        eventStrategy.execute(this.loadSagaContext(event));
        this.inboxEventService.register(event, sourceTopic);
    }

    private EventStrategy<EventTypeEnum, PurchaseSagaContextDto> resolveStrategy(
            EventStrategyRegistry<EventTypeEnum, PurchaseSagaContextDto> strategyRegistry,
            String eventType,
            String sourceTopic
    ) {
        return EventTypeEnum.fromValue(eventType)
                .map(strategyRegistry::find)
                .orElseThrow(() -> new NonRetryableKafkaException(
                        "Event type " + eventType + " is not supported by " + sourceTopic, null));
    }

    private PurchaseSagaContextDto loadSagaContext(IntegrationEventDto event) {
        Long orderId = Long.valueOf(event.aggregateId());
        OrderEntity orderEntity = this.orderRepository.findById(orderId).orElseThrow();
        PurchaseSagaEntity purchaseSagaEntity = this.purchaseSagaRepository.findByOrderId(orderId).orElseThrow();

        return PurchaseSagaContextDto.builder()
                .orderEntity(orderEntity)
                .purchaseSagaEntity(purchaseSagaEntity)
                .event(event)
                .build();
    }

}
