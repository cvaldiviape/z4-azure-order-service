package com.z4greed.order.service.saga.purchase.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.entity.ProcessedEventEntity;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.mapper.ProcessedEventMapper;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.ProcessedEventRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.service.saga.purchase.PurchaseSagaService;
import com.z4greed.order.service.saga.purchase.strategy.PurchaseSagaEventStrategy;
import com.z4greed.order.service.saga.purchase.strategy.PurchaseSagaEventStrategyRegistry;
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
  private final PurchaseSagaEventStrategyRegistry eventStrategyRegistry;
  private final ObjectMapper mapper;

  public PurchaseSagaServiceImpl(
      OrderRepository orderRepository,
      PurchaseSagaRepository purchaseSagaRepository,
      ProcessedEventRepository processedEventRepository,
      ProcessedEventMapper processedEventMapper,
      PurchaseSagaEventStrategyRegistry eventStrategyRegistry,
      ObjectMapper mapper
  ) {
    this.orderRepository = orderRepository;
    this.purchaseSagaRepository = purchaseSagaRepository;
    this.processedEventRepository = processedEventRepository;
    this.processedEventMapper = processedEventMapper;
    this.eventStrategyRegistry = eventStrategyRegistry;
    this.mapper = mapper;
  }

  @Override
  public void handleInventoryEvent(String rawEvent) {
    this.handleEvent(rawEvent);
  }

  @Override
  public void handlePaymentEvent(String rawEvent) {
    this.handleEvent(rawEvent);
  }

  private void handleEvent(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    log.info("action=event_received eventType={} eventId={} correlationId={} orderId={} producer={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), eventEnvelopeDto.producer());

    try {
      this.processEvent(eventEnvelopeDto);
    } catch (RuntimeException exception) {
      log.error("action=event_processing_failed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception);
      throw exception;
    }
  }

  private void processEvent(EventEnvelopeDto eventEnvelopeDto) {
    Boolean wasProcessed = this.wasProcessed(eventEnvelopeDto);

    if (wasProcessed) {
      log.info("action=event_ignored reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }

    PurchaseSagaEventStrategy eventStrategy = this.findEventStrategy(eventEnvelopeDto);

    if (eventStrategy == null) {
      log.info("action=event_ignored reason=unsupported_event_type eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }

    PurchaseSagaContextDto purchaseSagaContextDto = this.loadSagaContext(eventEnvelopeDto);
    eventStrategy.execute(purchaseSagaContextDto);
    this.markAsProcessed(eventEnvelopeDto);
    log.info("action=event_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      log.error("action=event_deserialization_failed message=Invalid_Kafka_event", exception);
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private Boolean wasProcessed(EventEnvelopeDto eventEnvelopeDto) {
    return this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
  }

  private PurchaseSagaEventStrategy findEventStrategy(EventEnvelopeDto eventEnvelopeDto) {
    String eventType = eventEnvelopeDto.eventType();
    Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventType);

    return eventTypeEnum
        .map(this.eventStrategyRegistry::find)
        .orElse(null);
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
