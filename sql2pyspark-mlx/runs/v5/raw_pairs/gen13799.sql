WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        log_ts,
        level,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(level, 1, 4) AS level_prefix
    FROM flattened_attrs
    WHERE log_ts >= TIMESTAMP '2025-06-01 14:00:00'
      AND status_code_high > 2
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        log_ts,
        level,
        has_fault,
        status_code_high,
        level_prefix,
        CASE WHEN level = 'WARN' THEN 1 ELSE 0 END AS is_warn
    FROM valid
    WHERE log_ts > DATE '2025-06-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS status_code_high_dense_rank,
        SUM(has_fault) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS has_fault_sum_2h,
        AVG(status_code_high) OVER w_grp AS status_code_high_avg_1h,
        CUME_DIST() OVER w_recent AS has_fault_cume_dist
    FROM valid_app_logs
    WINDOW w_hist AS (PARTITION BY level ORDER BY status_code_high DESC), w_grp AS (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY level ORDER BY has_fault NULLS FIRST)
),
valid_app_logs_2 AS (
    -- Basic cleansing and defaults
    SELECT
        log_ts,
        level,
        status_code_high,
        level_prefix,
        is_warn,
        status_code_high_dense_rank,
        has_fault_sum_2h,
        status_code_high_avg_1h,
        ROUND(is_warn / status_code_high_dense_rank, 2) AS is_warn_per_status_code_high_dense_rank,
        YEAR(log_ts) AS log_ts_year
    FROM enriched
    WHERE log_ts > TIMESTAMP '2025-06-01 10:00:00'
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_tier,
    CASE WHEN status_code_high_avg_1h >= 3 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_avg_1h_tier,
    CASE WHEN has_fault_sum_2h >= 4 THEN 'LARGE' ELSE 'SMALL' END AS has_fault_sum_2h_tier,
    SIZE(COLLECT_SET(level)) AS num_levels,
    ROUND(SUM(status_code_high_dense_rank) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS status_code_high_dense_rank_per_debug,
    COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count,
    COUNT(*) FILTER (WHERE level = 'DEBUG') AS debug_rows
FROM valid_app_logs_2
GROUP BY status_code_high_tier, status_code_high_avg_1h_tier, has_fault_sum_2h_tier
ORDER BY status_code_high_tier DESC
