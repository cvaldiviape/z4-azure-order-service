package com.z4greed.order.kafka.producer;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.exception.CustomNonRetryableKafkaException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper mapper;

  public OrderEventProducer(
      KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.mapper = mapper;
  }

  public void publish(String topic, EventEnvelopeDto event) {
    try {
      String eventJson = this.mapper.writeValueAsString(event);
      this.kafkaTemplate.send(topic, event.aggregateId(), eventJson).whenComplete((sendResult, exception) -> {
        if (exception != null) {
          log.error("action=event_publish_failed topic={} eventType={} eventId={} correlationId={} orderId={}", topic, event.eventType(), event.eventId(), event.correlationId(), event.aggregateId(), exception);
          return;
        }

        log.info("action=event_published topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={}", topic, sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset(), event.eventType(), event.eventId(), event.correlationId(), event.aggregateId());
      });
    } catch (Exception exception) {
      log.error("action=event_serialization_failed topic={} eventType={} eventId={} correlationId={} orderId={}", topic, event.eventType(), event.eventId(), event.correlationId(), event.aggregateId(), exception);
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}
