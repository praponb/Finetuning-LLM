WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) AS prev_log_ts
    FROM flattened
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        value,
        SUM(is_new_burst) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        status_code,
        k,
        UPPER(service) AS service_upper,
        GREATEST(burst_id, status_code) AS greatest_burst_id_status_code,
        COALESCE(k, 'DIRECT') AS resolved_k
    FROM islands
    WHERE log_ts >= DATE '2025-06-01'
      AND k IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(k) AS k_length
    FROM valid
    WHERE k IS NOT NULL
)
-- Grouped metrics
SELECT
    k,
    service,
    SUM(k_length) FILTER (WHERE k = 'version') AS version_total_k_length,
    SIZE(COLLECT_SET(k)) AS num_ks,
    COUNT_IF(service = 'auth') AS auth_events
FROM cleaned
GROUP BY k, service
ORDER BY num_ks DESC NULLS LAST;
