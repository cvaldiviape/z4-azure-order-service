ALTER TABLE orders
    ALTER COLUMN payment_token DROP NOT NULL,
    ADD CONSTRAINT uk_orders_correlation_id UNIQUE (correlation_id),
    ADD CONSTRAINT chk_orders_total_amount CHECK (total_amount >= 0),
    ADD CONSTRAINT chk_orders_status CHECK (status IN ('CREATED', 'STOCK_RESERVED', 'PAYMENT_PENDING', 'CONFIRMED', 'CANCELLED'));

ALTER TABLE order_items
    ADD CONSTRAINT chk_order_items_unit_price CHECK (unit_price >= 0),
    ADD CONSTRAINT chk_order_items_quantity CHECK (quantity > 0),
    ADD CONSTRAINT chk_order_items_subtotal CHECK (subtotal >= 0);

ALTER TABLE purchase_sagas
    ADD CONSTRAINT chk_purchase_sagas_status CHECK (status IN ('STARTED', 'IN_PROGRESS', 'COMPENSATING', 'COMPLETED', 'FAILED'));

CREATE INDEX idx_orders_customer_created_at ON orders (customer_id, created_at);
CREATE INDEX idx_orders_status ON orders (status);
CREATE INDEX idx_purchase_sagas_status_updated_at ON purchase_sagas (status, updated_at);

ALTER TABLE inbox_events
    ADD COLUMN aggregate_id VARCHAR(100),
    ADD COLUMN correlation_id VARCHAR(100),
    ADD COLUMN causation_id VARCHAR(100),
    ADD COLUMN producer VARCHAR(100),
    ADD COLUMN source_topic VARCHAR(200);

CREATE INDEX idx_inbox_events_correlation_id ON inbox_events (correlation_id);
