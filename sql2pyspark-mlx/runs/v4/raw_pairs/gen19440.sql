WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        latency_ms,
        key,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM flattened
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        latency_ms,
        value,
        status_code_high,
        is_flagged,
        TRIM(key) AS clean_key
    FROM flattened_attrs
    WHERE log_ts > TIMESTAMP '2025-06-01 06:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_high_max_1h,
        AVG(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_high_avg_2h,
        PERCENT_RANK() OVER (ORDER BY status_code_high) AS status_code_high_percent_rank
    FROM valid
)
SELECT
    log_ts,
    latency_ms,
    status_code_high,
    is_flagged,
    clean_key,
    status_code_high_max_1h,
    status_code_high_avg_2h,
    status_code_high_percent_rank
FROM metrics
