WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) latency_ms_share,
        CUME_DIST() OVER (ORDER BY status_code DESC) status_code_cume_dist
    FROM flattened
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        status_code_cume_dist,
        ROUND(latency_ms * 0.85, 2) latency_ms_scaled,
        COALESCE(k, 'N/A') k_filled
    FROM enriched
    WHERE log_ts >= TIMESTAMP '2025-06-01 15:00:00'
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), log_ts) days_since_log_ts
    FROM valid
    WHERE log_ts > DATE '2025-06-02'
      AND k IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY service ORDER BY status_code) AS latest_rank
    FROM valid_app_logs
    QUALIFY latest_rank = 1
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    host,
    log_ts,
    status_code,
    k,
    status_code_cume_dist,
    latency_ms_scaled,
    k_filled,
    days_since_log_ts,
    latest_rank,
    FIRST_VALUE(status_code_cume_dist) IGNORE NULLS OVER (PARTITION BY host ORDER BY latency_ms_scaled, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status_code_cume_dist,
    LAG(days_since_log_ts, 2) OVER (ORDER BY latest_rank, id, k) prev_days_since_log_ts
FROM deduped
