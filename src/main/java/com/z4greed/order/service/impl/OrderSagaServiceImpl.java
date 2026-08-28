package com.z4greed.order.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.order.dto.SagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.OrderSagaEntity;
import com.z4greed.order.entity.ProcessedEventEntity;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.mapper.ProcessedEventMapper;
import com.z4greed.order.repository.OrderRepository;
import com.z4greed.order.repository.ProcessedEventRepository;
import com.z4greed.order.repository.SagaRepository;
import com.z4greed.order.service.OrderSagaService;
import com.z4greed.order.strategy.OrderSagaEventStrategy;
import com.z4greed.order.strategy.OrderSagaEventStrategyRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

@Service
@Transactional
public class OrderSagaServiceImpl implements OrderSagaService {
  private final OrderRepository orderRepository;
  private final SagaRepository sagaRepository;
  private final ProcessedEventRepository processedEventRepository;
  private final ProcessedEventMapper processedEventMapper;
  private final OrderSagaEventStrategyRegistry eventStrategyRegistry;
  private final ObjectMapper mapper;

  public OrderSagaServiceImpl(
      OrderRepository orderRepository,
      SagaRepository sagaRepository,
      ProcessedEventRepository processedEventRepository,
      ProcessedEventMapper processedEventMapper,
      OrderSagaEventStrategyRegistry eventStrategyRegistry,
      ObjectMapper mapper
  ) {
    this.orderRepository = orderRepository;
    this.sagaRepository = sagaRepository;
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
    Boolean wasProcessed = this.wasProcessed(eventEnvelopeDto);

    if (wasProcessed) {
      return;
    }

    OrderSagaEventStrategy eventStrategy = this.findEventStrategy(eventEnvelopeDto);

    if (eventStrategy == null) {
      return;
    }

    SagaContextDto sagaContextDto = this.loadSagaContext(eventEnvelopeDto);
    eventStrategy.execute(sagaContextDto);
    this.markAsProcessed(eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private Boolean wasProcessed(EventEnvelopeDto eventEnvelopeDto) {
    return this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
  }

  private OrderSagaEventStrategy findEventStrategy(EventEnvelopeDto eventEnvelopeDto) {
    String eventType = eventEnvelopeDto.eventType();
    Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventType);

    return eventTypeEnum
        .map(this.eventStrategyRegistry::find)
        .orElse(null);
  }

  private SagaContextDto loadSagaContext(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    OrderEntity orderEntity = this.orderRepository.findById(orderId).orElseThrow();
    OrderSagaEntity orderSagaEntity = this.sagaRepository.findByOrderId(orderId).orElseThrow();

    return SagaContextDto.builder()
        .orderEntity(orderEntity)
        .orderSagaEntity(orderSagaEntity)
        .sourceEvent(eventEnvelopeDto)
        .build();
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = this.processedEventMapper.toEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }
}
