WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(status_code) AS status_code_whole,
        CAST(status_code AS STRING) AS status_code_str
    FROM lines
    WHERE log_ts >= DATE '2025-06-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        attr_key,
        status_code_whole,
        status_code_str,
        AVG(status_code_whole) OVER (PARTITION BY attr_key) AS attr_key_avg_status_code_whole,
        AVG(status_code_whole) OVER (PARTITION BY host) AS host_avg_status_code_whole,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code DESC) AS status_code_cume_dist,
        AVG(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m
    FROM base
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(attr_key, 'Unknown') AS attr_key_filled,
        SUBSTRING(attr_key, 1, 4) AS attr_key_prefix
    FROM with_windows
    WHERE attr_key IS DISTINCT FROM 'region'
)
-- Running and moving aggregates
SELECT
    id,
    status_code,
    status_code_str,
    status_code_cume_dist,
    status_code_avg_15m,
    attr_key_filled,
    attr_key_prefix,
    COUNT(status_code_cume_dist) OVER (PARTITION BY attr_key) AS attr_key_count_status_code_cume_dist,
    PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code_whole DESC NULLS LAST) AS status_code_whole_percent_rank
FROM valid
