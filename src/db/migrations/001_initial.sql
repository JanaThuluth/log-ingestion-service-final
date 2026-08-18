CREATE TABLE IF NOT EXISTS logs (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    level TEXT NOT NULL,
    service TEXT NOT NULL,
    message TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_logs_timestamp_id
    ON logs (timestamp DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_logs_service_timestamp_id
    ON logs (service, timestamp DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_logs_level_timestamp_id
    ON logs (level, timestamp DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_logs_attributes
    ON logs USING GIN (attributes);