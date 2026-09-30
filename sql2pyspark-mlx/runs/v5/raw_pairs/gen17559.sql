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
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        level,
        attr_key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM flattened
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        level,
        status_code_high,
        RANK() OVER (PARTITION BY level, attr_key ORDER BY status_code_high) AS status_code_high_rank,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_percent_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_high NULLS LAST) AS status_code_high_percent_rank_2,
        STDDEV_SAMP(has_fault) OVER (PARTITION BY level, attr_key) AS level_stddev_samp_has_fault,
        MIN(status_code_high) OVER (PARTITION BY attr_key) AS attr_key_min_status_code_high
    FROM parsed
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(status_code_high * 0.85, 2) AS status_code_high_adjusted
    FROM with_windows
    WHERE status_code_high_rank > 7
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(status_code_high_percent_rank) AS status_code_high_percent_rank_whole
    FROM valid
    WHERE level = 'DEBUG'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        level,
        SUM(status_code_high_percent_rank_2) AS total_status_code_high_percent_rank_2,
        PERCENTILE_APPROX(attr_key_min_status_code_high, ARRAY(0.25, 0.5, 0.75)) AS attr_key_min_status_code_high_quartiles
    FROM cleaned
    GROUP BY 1
)
SELECT
    level,
    total_status_code_high_percent_rank_2,
    attr_key_min_status_code_high_quartiles
FROM totals
