package com.z4greed.order.kafka.config;

import com.z4greed.order.exception.CustomRetryableKafkaException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConsumerRetryConfig {
  private static final String DLT_SUFFIX = "-dlt";

  private final Long retryIntervalMilliseconds;
  private final Long maxAttempts;

  public KafkaConsumerRetryConfig(
      @Value("${app.kafka.retry.interval-milliseconds}") Long retryIntervalMilliseconds,
      @Value("${app.kafka.retry.max-attempts}") Long maxAttempts) {
    this.retryIntervalMilliseconds = retryIntervalMilliseconds;
    this.maxAttempts = maxAttempts;
  }

  // Spring Boot detecta este bean y lo asigna automáticamente a los @KafkaListener de este backend.
  // Si el listener lanza una excepción, aplica la espera configurada, reintenta y finalmente usa el recoverer.
  @Bean
  public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = this.buildDeadLetterPublishingRecoverer(kafkaTemplate);
    long maxRetries = Math.max(this.maxAttempts - 1L, 0L);
    FixedBackOff fixedBackOff = new FixedBackOff(this.retryIntervalMilliseconds, maxRetries);
    DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer, fixedBackOff);
    // La política es restrictiva: cualquier excepción desconocida va directamente al DLT.
    // Solo se reintentan fallos que declaramos explícitamente como temporales o recuperables.
    defaultErrorHandler.defaultFalse();
    defaultErrorHandler.addRetryableExceptions(
        CustomRetryableKafkaException.class,
        TransientDataAccessException.class,
        RecoverableDataAccessException.class);


    defaultErrorHandler.setRetryListeners(this::logRetryAttempt);
    return defaultErrorHandler;
  }

  // Publica en el DLT el mismo key y value que no pudieron procesarse y agrega headers con el error original.
  // Si esta publicación también falla, la excepción se conserva para evitar confirmar y perder el mensaje original.
  private DeadLetterPublishingRecoverer buildDeadLetterPublishingRecoverer(KafkaTemplate<String, String> kafkaTemplate) {
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, this::resolveDeadLetterDestination);
    deadLetterPublishingRecoverer.setFailIfSendResultIsError(true);
    return deadLetterPublishingRecoverer;
  }

  // Obtiene dinámicamente el topic donde llegó el mensaje y le agrega el sufijo "-dlt".
  // Ejemplo: inventory-events-topic se convierte en inventory-events-topic-dlt y conserva la partición.
  private TopicPartition resolveDeadLetterDestination(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
    String deadLetterTopic = consumerRecord.topic() + DLT_SUFFIX;
    String exceptionType = exception.getClass().getSimpleName();
    String errorMessage = exception.getMessage();
    log.error("action=event_sent_to_dlt sourceTopic={} topic={} partition={} offset={} key={} exceptionType={} errorMessage=\"{}\"", consumerRecord.topic(), deadLetterTopic, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), exceptionType, errorMessage);
    return new TopicPartition(deadLetterTopic, consumerRecord.partition());
  }

  // Registra cada entrega fallida para conocer el intento, topic, partición y offset investigados.
  private void logRetryAttempt(ConsumerRecord<?, ?> consumerRecord, Exception exception, Integer deliveryAttempt) {
    String exceptionType = exception.getClass().getSimpleName();
    String errorMessage = exception.getMessage();
    log.warn("action=event_retry topic={} partition={} offset={} key={} deliveryAttempt={} maxAttempts={} exceptionType={} errorMessage=\"{}\"", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), deliveryAttempt, this.maxAttempts, exceptionType, errorMessage);
  }

  // Declara los DLT correspondientes a los topics que consume Order Service.
  // Los reintentos no crean otros topics: DefaultErrorHandler vuelve a procesar el mensaje en su
  // topic de origen. Solo después de agotar los intentos lo publica en el DLT correspondiente.
  // La relación es por nombre: inventory-events-topic + "-dlt" y payments-events-topic + "-dlt".
  @Bean
  public NewTopics purchaseSagaDeadLetterTopics() {
    NewTopic newTopic1 = TopicBuilder.name("inventory-events-topic-dlt")
            .partitions(1)
            .replicas(1)
            .build();
    NewTopic newTopic2 = TopicBuilder.name("payments-events-topic-dlt")
            .partitions(1)
            .replicas(1)
            .build();

    return new NewTopics(newTopic1, newTopic2);
  }

}