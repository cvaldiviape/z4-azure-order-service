package com.z4greed.order.kafka.producer;

import com.z4greed.order.entity.OutboxEventEntity;
import com.z4greed.order.exception.CustomRetryableKafkaException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final long sendTimeoutMilliseconds;

  public OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate, @Value("${app.outbox.send-timeout-milliseconds}") long sendTimeoutMilliseconds) {
    this.kafkaTemplate = kafkaTemplate;
    this.sendTimeoutMilliseconds = sendTimeoutMilliseconds;
  }

  public void publishAndWait(OutboxEventEntity outboxEvent) {
    String topic = outboxEvent.getTopic();
    String eventKey = outboxEvent.getEventKey();
    String payload = outboxEvent.getPayload();

    try {
      // send() inicia la publicación de manera asíncrona: devuelve inmediatamente un Future
      // que representa una operación todavía pendiente, no una confirmación del consumidor.
      CompletableFuture<SendResult<String, String>> send = this.kafkaTemplate.send(topic, eventKey, payload);

      // get() espera hasta que Kafka confirme la escritura (ACK) o venza el timeout.
      // Si retorna un SendResult, Kafka asignó una partición y un offset al mensaje.
      // Si Kafka rechaza el envío o no responde, get() lanza una excepción.
      SendResult<String, String> sendResult = send.get(this.sendTimeoutMilliseconds, TimeUnit.MILLISECONDS);

      printLogInfo(outboxEvent, topic, sendResult);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new CustomRetryableKafkaException("Kafka publication was interrupted", exception);
    } catch (Exception exception) {
      throw new CustomRetryableKafkaException("Kafka did not confirm the outbox outboxEvent", exception);
    }
  }

  private static void printLogInfo(OutboxEventEntity outboxEvent, String topic, SendResult<String, String> sendResult) {
    log.info("action=event_published topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={}",
            topic, sendResult.getRecordMetadata().partition(),
            sendResult.getRecordMetadata().offset(),
            outboxEvent.getEventType(),
            outboxEvent.getEventId(),
            outboxEvent.getCorrelationId(),
            outboxEvent.getAggregateId()
    );
  }

}