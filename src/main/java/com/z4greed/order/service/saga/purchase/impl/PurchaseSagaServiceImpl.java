package com.z4greed.order.service.saga.purchase.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

@Service
@Transactional
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
    Boolean wasProcessed = this.wasProcessed(eventEnvelopeDto);

    if (wasProcessed) {
      return;
    }

    PurchaseSagaEventStrategy eventStrategy = this.findEventStrategy(eventEnvelopeDto);

    if (eventStrategy == null) {
      return;
    }

    PurchaseSagaContextDto purchaseSagaContextDto = this.loadSagaContext(eventEnvelopeDto);
    eventStrategy.execute(purchaseSagaContextDto);
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