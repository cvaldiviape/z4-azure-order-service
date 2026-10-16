ALTER TABLE inbox_events
    RENAME COLUMN causation_id TO previous_event_id;

UPDATE outbox_events
SET payload = REPLACE(payload, '"causationId":', '"previousEventId":')
WHERE status = 'PENDING'
  AND payload LIKE '%"causationId":%';
