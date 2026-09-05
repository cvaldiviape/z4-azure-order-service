package com.z4greed.order.service.saga.purchase.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.entity.ProcessedEventEntity;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.exception.CustomNonRetryableKafkaException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.mapper.ProcessedEventMapper;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.ProcessedEventRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.service.saga.purchase.PurchaseSagaService;
import com.z4greed.order.service.saga.purchase.strategy.InventoryEventStrategyRegistry;
import com.z4greed.order.service.saga.purchase.strategy.PaymentEventStrategyRegistry;
import com.z4greed.order.service.saga.purchase.strategy.PurchaseSagaEventStrategy;
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
  private final ProcessedEventRepository processedEventRepository;
  private final ProcessedEventMapper processedEventMapper;
  private final InventoryEventStrategyRegistry inventoryEventStrategyRegistry;
  private final PaymentEventStrategyRegistry paymentEventStrategyRegistry;
  private final ObjectMapper mapper;

  public PurchaseSagaServiceImpl(
      OrderRepository orderRepository,
      PurchaseSagaRepository purchaseSagaRepository,
      ProcessedEventRepository processedEventRepository,
      ProcessedEventMapper processedEventMapper,
      InventoryEventStrategyRegistry inventoryEventStrategyRegistry,
      PaymentEventStrategyRegistry paymentEventStrategyRegistry,
      ObjectMapper mapper
  ) {
    this.orderRepository = orderRepository;
    this.purchaseSagaRepository = purchaseSagaRepository;
    this.processedEventRepository = processedEventRepository;
    this.processedEventMapper = processedEventMapper;
    this.inventoryEventStrategyRegistry = inventoryEventStrategyRegistry;
    this.paymentEventStrategyRegistry = paymentEventStrategyRegistry;
    this.mapper = mapper;
  }

  @Override
  public void handleInventoryEvent(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    log.info("action=event_received sourceTopic=inventory-events-topic eventType={} eventId={} correlationId={} orderId={} producer={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), eventEnvelopeDto.producer());

    try {
      if (this.wasProcessed(eventEnvelopeDto)) {
        log.info("action=event_ignored sourceTopic=inventory-events-topic reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
        return;
      }

      Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventEnvelopeDto.eventType());
      PurchaseSagaEventStrategy eventStrategy = eventTypeEnum
          .map(this.inventoryEventStrategyRegistry::find)
          .orElse(null);

      if (eventStrategy == null) {
        throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, new IllegalArgumentException("Event type " + eventEnvelopeDto.eventType() + " is not supported by inventory-events-topic"));
      }

      PurchaseSagaContextDto purchaseSagaContextDto = this.loadSagaContext(eventEnvelopeDto);
      eventStrategy.execute(purchaseSagaContextDto);
      this.markAsProcessed(eventEnvelopeDto);
      log.info("action=event_processed sourceTopic=inventory-events-topic eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
    } catch (RuntimeException exception) {
      log.error("action=event_processing_failed sourceTopic=inventory-events-topic eventType={} eventId={} correlationId={} orderId={} exceptionType={} errorMessage=\"{}\"", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
      throw exception;
    }
  }

  @Override
  public void handlePaymentEvent(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    log.info("action=event_received sourceTopic=payments-events-topic eventType={} eventId={} correlationId={} orderId={} producer={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), eventEnvelopeDto.producer());

    try {
      if (this.wasProcessed(eventEnvelopeDto)) {
        log.info("action=event_ignored sourceTopic=payments-events-topic reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
        return;
      }

      Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventEnvelopeDto.eventType());
      PurchaseSagaEventStrategy eventStrategy = eventTypeEnum
          .map(this.paymentEventStrategyRegistry::find)
          .orElse(null);

      if (eventStrategy == null) {
        throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, new IllegalArgumentException("Event type " + eventEnvelopeDto.eventType() + " is not supported by payments-events-topic"));
      }

      PurchaseSagaContextDto purchaseSagaContextDto = this.loadSagaContext(eventEnvelopeDto);
      eventStrategy.execute(purchaseSagaContextDto);

      this.markAsProcessed(eventEnvelopeDto);
      log.info("action=event_processed sourceTopic=payments-events-topic eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
    } catch (RuntimeException exception) {
      log.error("action=event_processing_failed sourceTopic=payments-events-topic eventType={} eventId={} correlationId={} orderId={} exceptionType={} errorMessage=\"{}\"", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
      throw exception;
    }
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      log.error("action=event_deserialization_failed message=Invalid_Kafka_event", exception);
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private Boolean wasProcessed(EventEnvelopeDto eventEnvelopeDto) {
    return this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
  }

  private PurchaseSagaContextDto loadSagaContext(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    OrderEntity orderEntity = this.orderRepository.findById(orderId).orElseThrow();
    PurchaseSagaEntity purchaseSagaEntity = this.purchaseSagaRepository.findByOrderId(orderId).orElseThrow();

    return PurchaseSagaContextDto.builder()
        .orderEntity(orderEntity)
        .purchaseSagaEntity(purchaseSagaEntity)
        .sourceEvent(eventEnvelopeDto)
        .build();
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = this.processedEventMapper.toEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }

}