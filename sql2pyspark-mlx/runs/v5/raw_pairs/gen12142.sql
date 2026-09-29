WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) status_code_avg_30m,
        AVG(status_code) OVER (PARTITION BY level) level_avg_status_code
    FROM flattened
    WHERE level NOT IN ('WARN', 'DEBUG')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        log_ts,
        level,
        status_code,
        status_code_avg_30m,
        level_avg_status_code,
        ROUND(status_code, 1) status_code_rounded
    FROM with_windows
    WHERE log_ts >= TIMESTAMP '2025-06-01 08:00:00'
      AND log_ts >= '2023-07-15'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_code_avg_30m) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) status_code_avg_30m_max_15m,
        DENSE_RANK() OVER (ORDER BY status_code_rounded DESC NULLS LAST) status_code_rounded_dense_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) status_code_share,
        COUNT(status_code_avg_30m) OVER (PARTITION BY level, service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) status_code_avg_30m_count_2h
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_code_rounded) OVER (PARTITION BY service) service_max_status_code_rounded,
        ROUND(status_code_avg_30m_count_2h / SUM(status_code_avg_30m_count_2h) OVER (PARTITION BY level), 4) status_code_avg_30m_count_2h_share,
        AVG(status_code_avg_30m_count_2h) OVER w_hist status_code_avg_30m_count_2h_avg_30m,
        AVG(status_code_avg_30m) OVER w_hist_2 status_code_avg_30m_avg_1h
    FROM metrics
    WINDOW w_hist AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY status_code_share ASC NULLS FIRST) status_code_share_percent_rank,
        CUME_DIST() OVER (ORDER BY status_code_avg_30m ASC NULLS FIRST) status_code_avg_30m_cume_dist,
        DENSE_RANK() OVER (ORDER BY status_code_avg_30m_max_15m ASC NULLS LAST) status_code_avg_30m_max_15m_dense_rank,
        COUNT(status_code_rounded) OVER (PARTITION BY level) level_count_status_code_rounded,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code_rounded DESC) status_code_rounded_dense_rank_2
    FROM enriched
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        level,
        service,
        level_count_status_code_rounded
    FROM metrics_app_logs
) src
PIVOT (
    AVG(level_count_status_code_rounded) AS amount,
    COUNT(1) AS cnt
    FOR service IN ('billing', 'auth')
);
