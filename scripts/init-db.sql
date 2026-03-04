-- ============================================================
-- PostgreSQL Schema Initialization
-- Kaskade - Real-Time Market Analytics Engine
-- ============================================================
-- Creates the core tables for persistent storage of market
-- metrics and alert audit logs, with indexes optimized for
-- Grafana time-series queries.
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- Table: market_metrics
-- Stores windowed aggregation results from Spark Structured
-- Streaming. Partitioned by day for efficient time-range queries.
-- ============================================================
CREATE TABLE IF NOT EXISTS market_metrics (
    id            BIGSERIAL PRIMARY KEY,
    symbol        VARCHAR(10) NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    vwap          NUMERIC(18, 6),
    volatility    NUMERIC(18, 8),
    rsi           NUMERIC(5, 2),
    volume        BIGINT,
    trade_count   INTEGER,
    high          NUMERIC(18, 6),
    low           NUMERIC(18, 6),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- B-tree index on (symbol, window_start) for Grafana time-series queries
CREATE INDEX IF NOT EXISTS idx_market_metrics_symbol_window
    ON market_metrics (symbol, window_start DESC);

-- Index for time-range scans (Grafana panels with $__timeFilter)
CREATE INDEX IF NOT EXISTS idx_market_metrics_window_start
    ON market_metrics (window_start DESC);

-- ============================================================
-- Table: alerts
-- Stores anomaly detection alert audit log. Each row represents
-- a detected anomaly event from the streaming pipeline.
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    id            BIGSERIAL PRIMARY KEY,
    symbol        VARCHAR(10) NOT NULL,
    alert_type    VARCHAR(50) NOT NULL,
    severity      VARCHAR(10) NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    z_score       NUMERIC(8, 4),
    threshold     NUMERIC(8, 4),
    current_value NUMERIC(18, 6),
    baseline_value NUMERIC(18, 6),
    message       TEXT,
    window_start  TIMESTAMPTZ,
    window_end    TIMESTAMPTZ,
    detected_at   TIMESTAMPTZ DEFAULT NOW(),
    acknowledged  BOOLEAN DEFAULT FALSE
);

-- Index for querying alerts by symbol and time
CREATE INDEX IF NOT EXISTS idx_alerts_symbol_detected
    ON alerts (symbol, detected_at DESC);

-- Index for filtering by severity (Grafana alert panels)
CREATE INDEX IF NOT EXISTS idx_alerts_severity
    ON alerts (severity, detected_at DESC);

-- Index for unacknowledged alerts
CREATE INDEX IF NOT EXISTS idx_alerts_unacknowledged
    ON alerts (acknowledged, detected_at DESC)
    WHERE acknowledged = FALSE;

-- ============================================================
-- Materialized View: latest_metrics
-- Pre-computed latest metrics per symbol for fast dashboard loads.
-- Refreshed periodically or on-demand by the API layer.
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_metrics AS
SELECT DISTINCT ON (symbol)
    symbol,
    window_start,
    window_end,
    vwap,
    volatility,
    rsi,
    volume,
    trade_count,
    high,
    low,
    created_at
FROM market_metrics
ORDER BY symbol, window_start DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_metrics_symbol
    ON latest_metrics (symbol);

-- ============================================================
-- Materialized View: alert_summary
-- Aggregated alert counts by symbol and severity for the
-- overview dashboard.
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS alert_summary AS
SELECT
    symbol,
    severity,
    alert_type,
    COUNT(*) AS alert_count,
    MAX(detected_at) AS last_detected,
    AVG(z_score) AS avg_z_score
FROM alerts
WHERE detected_at > NOW() - INTERVAL '24 hours'
GROUP BY symbol, severity, alert_type;

-- ============================================================
-- Table: pipeline_metrics
-- Operational metrics for monitoring pipeline health:
-- latency, throughput, and processing stats.
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id              BIGSERIAL PRIMARY KEY,
    metric_name     VARCHAR(100) NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    labels          JSONB DEFAULT '{}',
    recorded_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name_time
    ON pipeline_metrics (metric_name, recorded_at DESC);

-- Grant read access for Grafana queries
-- (Grafana connects with the same user in this setup)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO analytics;
