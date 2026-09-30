WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        attr_value,
        ROUND(status_code, 0) AS status_code_rounded,
        HOUR(log_ts) AS log_ts_hour,
        COALESCE(key, 'N/A') AS key_clean
    FROM flattened
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 11:00:00')
      AND log_ts < '2025-07-10'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        latency_ms / NULLIF(log_ts_hour, 0) AS latency_ms_per_log_ts_hour,
        COALESCE(key, 'UNKNOWN') AS key_filled
    FROM base
    WHERE service = 'auth'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        latency_ms,
        key,
        status_code_rounded,
        log_ts_hour,
        latency_ms_per_log_ts_hour,
        key_filled,
        UPPER(host) AS host_upper
    FROM valid
    WHERE key IS NOT NULL
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        status_code_rounded,
        latency_ms_per_log_ts_hour,
        key_filled,
        COALESCE(latency_ms_per_log_ts_hour, 0) AS latency_ms_per_log_ts_hour_filled,
        CONCAT_WS('-', service, key) AS service_key_key,
        COALESCE(key, 'NONE') AS key_filled_2
    FROM filtered
    WHERE key IS NOT NULL
      AND latency_ms_per_log_ts_hour > 75
),
metrics AS (
    -- Ranking and gap features
    SELECT
        service,
        latency_ms_per_log_ts_hour,
        key_filled,
        service_key_key,
        RANK() OVER (PARTITION BY service ORDER BY latency_ms_per_log_ts_hour_filled DESC NULLS FIRST) AS latency_ms_per_log_ts_hour_filled_rank
    FROM base_app_logs
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        key_filled,
        service_key_key,
        latency_ms_per_log_ts_hour_filled_rank,
        SUM(latency_ms_per_log_ts_hour_filled_rank) OVER w_ordered AS service_sum_latency_ms_per_log_ts_hour_filled_rank,
        SUM(latency_ms_per_log_ts_hour) OVER w_ordered AS service_sum_latency_ms_per_log_ts_hour
    FROM metrics
    WHERE latency_ms_per_log_ts_hour_filled_rank <= 7
    WINDOW w_ordered AS (PARTITION BY service)
)
SELECT
    service,
    key_filled,
    latency_ms_per_log_ts_hour_filled_rank,
    service_sum_latency_ms_per_log_ts_hour,
    CASE WHEN service RLIKE '^(auth|billing)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS service_zone
FROM enriched
ORDER BY latency_ms_per_log_ts_hour_filled_rank NULLS FIRST
