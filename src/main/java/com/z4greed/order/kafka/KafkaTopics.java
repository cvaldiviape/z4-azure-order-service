package com.z4greed.order.kafka;

// Nombres de los topics que usa Order Service. Centralizarlos evita literales repetidos y errores de tipeo.
public final class KafkaTopics {
    public static final String INVENTORY_RESERVATION_REQUESTS = "inventory-reservation-requests-topic";
    public static final String INVENTORY_RESERVATION_RESULTS = "inventory-reservation-results-topic";
    public static final String PAYMENT_PROCESSING_REQUESTS = "payment-processing-requests-topic";
    public static final String PAYMENT_PROCESSING_RESULTS = "payment-processing-results-topic";
    public static final String ORDER_STATUS_EVENTS = "order-status-events-topic";

    private KafkaTopics() {
    }
}
