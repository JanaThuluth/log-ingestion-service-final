CREATE OR REPLACE FUNCTION logs_attributes_kv(a jsonb) RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$
  SELECT array_agg(length(k)::text || ':' || k || '=' || v ORDER BY k)
  FROM LATERAL jsonb_each_text(a) AS kv(k, v)
$$;

CREATE INDEX IF NOT EXISTS idx_logs_attributes_kv
ON logs USING GIN (logs_attributes_kv(attributes));

DROP INDEX IF EXISTS idx_logs_attributes;

CREATE TABLE IF NOT EXISTS logs_rollup_1m (
    bucket_start TIMESTAMPTZ NOT NULL,
    service VARCHAR(255) NOT NULL,
    level VARCHAR(10) NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, service, level)
);

CREATE INDEX IF NOT EXISTS idx_rollup_1m_bucket ON logs_rollup_1m (bucket_start);