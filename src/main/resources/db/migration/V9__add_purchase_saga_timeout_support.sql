ALTER TABLE purchase_sagas
    ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE purchase_sagas
    DROP CONSTRAINT chk_purchase_sagas_status,
    ADD CONSTRAINT chk_purchase_sagas_status
        CHECK (status IN ('STARTED', 'IN_PROGRESS', 'COMPENSATING', 'TIMED_OUT', 'COMPLETED', 'FAILED'));

DROP INDEX idx_purchase_sagas_status_updated_at;

CREATE INDEX idx_purchase_sagas_active_last_activity
    ON purchase_sagas (status, COALESCE(updated_at, created_at))
    WHERE status IN ('STARTED', 'IN_PROGRESS', 'COMPENSATING');
