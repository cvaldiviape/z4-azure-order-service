package com.z4greed.order.kafka.producer;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.exception.GreedException;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
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
      this.kafkaTemplate.send(topic, event.aggregateId(), eventJson);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}
