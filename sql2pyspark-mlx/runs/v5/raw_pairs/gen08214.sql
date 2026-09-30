WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN service IN ('auth', 'checkout') THEN 'Priority' ELSE 'Other' END AS service_group
    FROM lines
    WHERE log_ts >= TIMESTAMP '2025-06-01 15:00:00'
      AND attr_key IS DISTINCT FROM 'version'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN level = 'INFO' THEN 1 ELSE 0 END AS is_info,
        UPPER(attr_key) AS attr_key_upper
    FROM valid
    WHERE level = 'ERROR'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        level,
        v,
        service_group,
        attr_key_upper,
        MAX(is_info) OVER (PARTITION BY level) AS level_max_is_info,
        MAX(is_info) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS is_info_max_15m,
        CUME_DIST() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_cume_dist
    FROM base
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        v,
        service_group,
        attr_key_upper,
        level_max_is_info,
        is_info_max_15m,
        latency_ms_cume_dist,
        latency_ms_cume_dist - is_info_max_15m AS latency_ms_cume_dist_minus_is_info_max_15m,
        GREATEST(level_max_is_info, is_info_max_15m) AS greatest_level_max_is_info_is_info_max_15m
    FROM metrics
    WHERE level != 'ERROR'
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY level_max_is_info) AS level_max_is_info_dense_rank,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY is_info_max_15m DESC NULLS LAST) AS is_info_max_15m_dense_rank
    FROM valid_app_logs
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY level ORDER BY latency_ms_cume_dist_minus_is_info_max_15m NULLS LAST) AS latency_ms_cume_dist_minus_is_info_max_15m_rank,
        MIN(is_info_max_15m) OVER (PARTITION BY level) AS level_min_is_info_max_15m,
        DENSE_RANK() OVER (ORDER BY is_info_max_15m_dense_rank DESC) AS is_info_max_15m_dense_rank_dense_rank
    FROM metrics_app_logs
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    level,
    kpi,
    metric_value
FROM (
    SELECT
        level,
        greatest_level_max_is_info_is_info_max_15m,
        latency_ms_cume_dist_minus_is_info_max_15m
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR kpi IN (greatest_level_max_is_info_is_info_max_15m AS GREATEST_LEVEL_MAX_IS_INFO_IS_INFO_MAX_15M_VAL, latency_ms_cume_dist_minus_is_info_max_15m)
) unp;
