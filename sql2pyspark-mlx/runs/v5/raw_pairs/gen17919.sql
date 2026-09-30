WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(latency_ms) AS latency_ms_whole
    FROM flattened
    WHERE attr_key <> 'version'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_sum_2h,
        COUNT(latency_ms_whole) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_whole_count_15m
    FROM base
    WHERE HOUR(log_ts) BETWEEN 9 AND 12
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(latency_ms_sum_2h, latency_ms_whole) AS greatest_latency_ms_sum_2h_latency_ms_whole,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM with_windows
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 13:00:00')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(latency_ms_whole_count_15m) OVER w_running AS latency_ms_whole_count_15m_avg_2h,
        AVG(greatest_latency_ms_sum_2h_latency_ms_whole) OVER w_recent AS attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole,
        MAX(latency_ms_whole) OVER w_recent AS attr_key_max_latency_ms_whole,
        MAX(latency_ms_whole) OVER (PARTITION BY level, service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_whole_max_2h,
        MAX(latency_ms) OVER w_recent_2 AS service_max_latency_ms
    FROM cleaned
    WINDOW w_running AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY attr_key), w_recent_2 AS (PARTITION BY service)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole - latency_ms_whole_count_15m AS attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole_minus_latency_ms_whole_count_15m,
        COALESCE(attr_key, 'DIRECT') AS attr_key_filled_2
    FROM scored
    WHERE service = 'billing'
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value,
        latency_ms_whole,
        latency_ms_sum_2h,
        greatest_latency_ms_sum_2h_latency_ms_whole,
        attr_key_filled,
        latency_ms_whole_count_15m_avg_2h,
        attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole,
        attr_key_max_latency_ms_whole,
        latency_ms_whole_max_2h,
        service_max_latency_ms,
        attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole_minus_latency_ms_whole_count_15m,
        attr_key_filled_2,
        latency_ms + attr_key_max_latency_ms_whole AS latency_ms_plus_attr_key_max_latency_ms_whole,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM valid
    WHERE attr_key IS NOT NULL
      AND latency_ms BETWEEN 893 AND 2061
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        level,
        service,
        attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole_minus_latency_ms_whole_count_15m
    FROM base_app_logs
) src
PIVOT (
    SUM(attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole_minus_latency_ms_whole_count_15m)
    FOR service IN ('checkout', 'search')
);
