package com.z4greed.order.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;

  public OrderEventProducer(
      KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
  }

  public void publish(String topic, EventEnvelopeDto event) {
    try {
      String eventJson = this.objectMapper.writeValueAsString(event);
      this.kafkaTemplate.send(topic, event.aggregateId(), eventJson);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}