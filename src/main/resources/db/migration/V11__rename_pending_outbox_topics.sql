UPDATE outbox_events
SET topic = CASE topic
    WHEN 'inventory-commands-topic' THEN 'inventory-reservation-requests-topic'
    WHEN 'payments-commands-topic' THEN 'payment-processing-requests-topic'
    WHEN 'orders-events-topic' THEN 'order-status-events-topic'
    ELSE topic
END
WHERE status = 'PENDING'
  AND topic IN (
      'inventory-commands-topic',
      'payments-commands-topic',
      'orders-events-topic'
  );
