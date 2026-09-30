WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        log_ts,
        latency_ms,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE log_ts >= '2025-08-25'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(latency_ms) latency_ms_whole,
        DATEDIFF(CURRENT_DATE(), log_ts) days_since_log_ts
    FROM unpacked
    WHERE latency_ms = 682
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        log_ts,
        is_flagged,
        status_code_high,
        days_since_log_ts,
        GREATEST(is_flagged, latency_ms) greatest_is_flagged_latency_ms,
        ROUND(days_since_log_ts, 2) days_since_log_ts_rounded
    FROM cleaned
    WHERE is_flagged > 0
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        log_ts,
        is_flagged,
        greatest_is_flagged_latency_ms,
        days_since_log_ts_rounded,
        MAX(status_code_high) OVER w_recent status_code_high_max_2h,
        COUNT(days_since_log_ts) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) days_since_log_ts_count_15m,
        DENSE_RANK() OVER (ORDER BY status_code_high) status_code_high_dense_rank,
        COUNT(days_since_log_ts) OVER w_recent days_since_log_ts_count_2h
    FROM valid
    WHERE is_flagged >= 0
    WINDOW w_recent AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        is_flagged,
        greatest_is_flagged_latency_ms,
        days_since_log_ts_rounded,
        status_code_high_max_2h,
        days_since_log_ts_count_15m,
        COUNT(days_since_log_ts_count_2h) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) days_since_log_ts_count_2h_count_30m,
        SUM(days_since_log_ts_rounded) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) days_since_log_ts_rounded_sum_15m,
        CUME_DIST() OVER (ORDER BY is_flagged DESC NULLS LAST) is_flagged_cume_dist,
        AVG(is_flagged) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) is_flagged_avg_1h
    FROM with_windows
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered days_since_log_ts_rounded_percent_rank,
        PERCENT_RANK() OVER (ORDER BY days_since_log_ts_count_2h_count_30m) days_since_log_ts_count_2h_count_30m_percent_rank,
        CUME_DIST() OVER (ORDER BY greatest_is_flagged_latency_ms DESC) greatest_is_flagged_latency_ms_cume_dist,
        DENSE_RANK() OVER w_recent days_since_log_ts_rounded_dense_rank
    FROM enriched
    WINDOW w_ordered AS (ORDER BY days_since_log_ts_rounded DESC), w_recent AS (ORDER BY days_since_log_ts_rounded DESC NULLS FIRST)
)
SELECT
    greatest_is_flagged_latency_ms,
    days_since_log_ts_rounded,
    status_code_high_max_2h,
    days_since_log_ts_count_15m,
    days_since_log_ts_count_2h_count_30m,
    days_since_log_ts_rounded_sum_15m,
    is_flagged_cume_dist,
    is_flagged_avg_1h,
    days_since_log_ts_rounded_percent_rank
FROM metrics
ORDER BY status_code_high_max_2h DESC NULLS FIRST
