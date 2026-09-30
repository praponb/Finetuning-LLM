WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        value,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE TO_DATE(log_ts) >= '2024-11-14'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY is_flagged NULLS LAST) AS is_flagged_rank,
        MAX(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_high_max_30m,
        CUME_DIST() OVER (ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_cume_dist,
        COUNT(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_high_count_2h,
        COUNT(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_high_count_2h_2
    FROM unpacked
    WHERE is_flagged != 0
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        value,
        status_code_high,
        is_flagged_rank,
        status_code_high_max_30m,
        status_code_high_count_2h,
        COUNT(is_flagged_rank) OVER w_running AS is_flagged_rank_count_15m,
        MAX(is_flagged) OVER w_running AS is_flagged_max_15m,
        COUNT(is_flagged) OVER w_running AS is_flagged_count_15m
    FROM enriched
    WINDOW w_running AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('DAY', log_ts) AS activity_day,
        MAX(is_flagged_rank_count_15m) AS max_is_flagged_rank_count_15m,
        ROUND(AVG(status_code_high), 2) AS avg_status_code_high,
        ROUND(AVG(status_code_high), 2) AS avg_status_code_high_2,
        COUNT(*) AS row_count
    FROM enriched_app_logs
    GROUP BY 1
)
SELECT
    activity_day,
    max_is_flagged_rank_count_15m,
    avg_status_code_high,
    avg_status_code_high_2,
    row_count
FROM grouped
ORDER BY avg_status_code_high DESC
