CREATE TABLE purchase_saga_histories (
    id BIGSERIAL PRIMARY KEY,
    purchase_saga_id BIGINT NOT NULL REFERENCES purchase_sagas(id),
    order_id BIGINT NOT NULL REFERENCES orders(id),
    order_status VARCHAR(30) NOT NULL,
    saga_status VARCHAR(30) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    error_message VARCHAR(500),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE INDEX idx_purchase_saga_histories_order_id
    ON purchase_saga_histories(order_id);

CREATE INDEX idx_purchase_saga_histories_purchase_saga_id
    ON purchase_saga_histories(purchase_saga_id);
