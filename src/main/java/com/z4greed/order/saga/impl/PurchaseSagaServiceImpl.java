package com.z4greed.order.saga.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.exception.CustomNonRetryableKafkaException;
import com.z4greed.order.kafka.event.IntegrationEventDto;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.saga.PurchaseSagaService;
import com.z4greed.order.service.InboxEventService;
import com.z4greed.order.strategy.StockEventStrategyRegistry;
import com.z4greed.order.strategy.PaymentEventStrategyRegistry;
import com.z4greed.order.strategy.EventStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

@Service
@Transactional
@Slf4j
public class PurchaseSagaServiceImpl implements PurchaseSagaService {
  private final OrderRepository orderRepository;
  private final PurchaseSagaRepository purchaseSagaRepository;
  private final InboxEventService inboxEventService;
  private final StockEventStrategyRegistry stockEventStrategyRegistry;
  private final PaymentEventStrategyRegistry paymentEventStrategyRegistry;
  private final ObjectMapper mapper;

  public PurchaseSagaServiceImpl(
      OrderRepository orderRepository,
      PurchaseSagaRepository purchaseSagaRepository,
      InboxEventService inboxEventService,
      StockEventStrategyRegistry stockEventStrategyRegistry,
      PaymentEventStrategyRegistry paymentEventStrategyRegistry,
      ObjectMapper mapper
  ) {
    this.orderRepository = orderRepository;
    this.purchaseSagaRepository = purchaseSagaRepository;
    this.inboxEventService = inboxEventService;
    this.stockEventStrategyRegistry = stockEventStrategyRegistry;
    this.paymentEventStrategyRegistry = paymentEventStrategyRegistry;
    this.mapper = mapper;
  }

  @Override
  public void handleInventoryEvent(String rawEvent) {
    IntegrationEventDto event = this.readEvent(rawEvent);

    String eventId = event.eventId();
    String eventType = event.eventType();
    String correlationId = event.correlationId();
    String aggregateId = event.aggregateId();
    String producer = event.producer();

    log.info("action=event_received sourceTopic=inventory-reservation-results-topic eventType={} eventId={} correlationId={} orderId={} producer={}", eventType, eventId, correlationId, aggregateId, producer);

    try {
      boolean wasAlreadyProcessed = this.inboxEventService.wasAlreadyProcessed(eventId);
      if (wasAlreadyProcessed) {
        log.info("action=event_ignored sourceTopic=inventory-reservation-results-topic reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventType, eventId, correlationId, aggregateId);
        return;
      }

      EventStrategy<EventTypeEnum, PurchaseSagaContextDto> eventStrategy = this.getStockEventStrategy(eventType);
      PurchaseSagaContextDto purchaseSagaContextDto = this.loadSagaContext(event);

      eventStrategy.execute(purchaseSagaContextDto);
      this.inboxEventService.register(event, "inventory-reservation-results-topic");

      log.info("action=event_processed sourceTopic=inventory-reservation-results-topic eventType={} eventId={} correlationId={} orderId={}", eventType, eventId, correlationId, aggregateId);
    } catch (RuntimeException exception) {
      String simpleName = exception.getClass().getSimpleName();
      String message = exception.getMessage();
      log.error("action=event_processing_failed sourceTopic=inventory-reservation-results-topic eventType={} eventId={} correlationId={} orderId={} exceptionType={} errorMessage=\"{}\"", eventType, eventId, correlationId, aggregateId, simpleName, message, exception);
      throw exception;
    }
  }

  @Override
  public void handlePaymentEvent(String rawEvent) {
    IntegrationEventDto event = this.readEvent(rawEvent);

    String eventId = event.eventId();
    String eventType = event.eventType();
    String correlationId = event.correlationId();
    String aggregateId = event.aggregateId();
    String producer = event.producer();

    log.info("action=event_received sourceTopic=payment-processing-results-topic eventType={} eventId={} correlationId={} orderId={} producer={}", eventType, eventId, correlationId, aggregateId, producer);

    try {
      boolean wasAlreadyProcessed = this.inboxEventService.wasAlreadyProcessed(eventId);
      if (wasAlreadyProcessed) {
        log.info("action=event_ignored sourceTopic=payment-processing-results-topic reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventType, eventId, correlationId, aggregateId);
        return;
      }

      EventStrategy<EventTypeEnum, PurchaseSagaContextDto> eventStrategy = this.getPaymentEventStrategy(eventType);
      PurchaseSagaContextDto purchaseSagaContextDto = this.loadSagaContext(event);

      eventStrategy.execute(purchaseSagaContextDto);
      this.inboxEventService.register(event, "payment-processing-results-topic");
      log.info("action=event_processed sourceTopic=payment-processing-results-topic eventType={} eventId={} correlationId={} orderId={}", eventType, eventId, correlationId, aggregateId);
    } catch (RuntimeException exception) {
      String simpleName = exception.getClass().getSimpleName();
      String message = exception.getMessage();
      log.error("action=event_processing_failed sourceTopic=payment-processing-results-topic eventType={} eventId={} correlationId={} orderId={} exceptionType={} errorMessage=\"{}\"", eventType, eventId, correlationId, aggregateId, simpleName, message, exception);
      throw exception;
    }
  }

  private IntegrationEventDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, IntegrationEventDto.class);
    } catch (Exception exception) {
      log.error("action=event_deserialization_failed message=Invalid_Kafka_event", exception);
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private EventStrategy<EventTypeEnum, PurchaseSagaContextDto> getStockEventStrategy(String eventType) {
    Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventType);

    EventStrategy<EventTypeEnum, PurchaseSagaContextDto> eventStrategy = eventTypeEnum
            .map(this.stockEventStrategyRegistry::find)
            .orElse(null);

    if (eventStrategy == null) {
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, new IllegalArgumentException("Event type " + eventType + " is not supported by inventory-reservation-results-topic"));
    }
    return eventStrategy;
  }

  private EventStrategy<EventTypeEnum, PurchaseSagaContextDto> getPaymentEventStrategy(String eventType) {

    Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventType);
    EventStrategy<EventTypeEnum, PurchaseSagaContextDto> eventStrategy = eventTypeEnum
            .map(this.paymentEventStrategyRegistry::find)
            .orElse(null);

    if (eventStrategy == null) {
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, new IllegalArgumentException("Event type " + eventType + " is not supported by payment-processing-results-topic"));
    }
    return eventStrategy;
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
