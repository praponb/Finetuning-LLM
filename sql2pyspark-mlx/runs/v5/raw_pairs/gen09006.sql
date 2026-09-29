WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN k = 'trace' THEN 1 ELSE 0 END AS is_trace,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        COALESCE(k, 'DIRECT') AS resolved_k
    FROM parsed
    WHERE status_code_high = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        is_trace + days_since_log_ts AS is_trace_plus_days_since_log_ts,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM filtered
    WHERE k IS NOT NULL
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        is_flagged,
        status_code_high,
        is_trace,
        days_since_log_ts,
        is_trace_plus_days_since_log_ts,
        k_filled,
        latency_ms + is_trace AS latency_ms_plus_is_trace,
        COALESCE(k, 'NONE') AS resolved_k_2
    FROM base
    WHERE k IS NOT NULL
      AND is_trace <= 65
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        latency_ms,
        is_flagged,
        status_code_high,
        days_since_log_ts,
        is_trace_plus_days_since_log_ts,
        latency_ms_plus_is_trace,
        resolved_k_2,
        LEAST(days_since_log_ts, is_trace) AS least_days_since_log_ts_is_trace,
        ROUND(status_code_high, 2) AS status_code_high_rounded
    FROM base_app_logs
    WHERE log_ts > TIMESTAMP '2025-06-01 11:00:00'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY status_code_high NULLS LAST) AS status_code_high_rank,
        AVG(is_flagged) OVER w_hist AS is_flagged_avg_1h,
        SUM(days_since_log_ts) OVER w_hist AS days_since_log_ts_sum_1h,
        SUM(least_days_since_log_ts_is_trace) OVER w_hist AS least_days_since_log_ts_is_trace_sum_1h,
        COUNT(least_days_since_log_ts_is_trace) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS least_days_since_log_ts_is_trace_count_2h
    FROM valid
    WINDOW w_hist AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    APPROX_PERCENTILE(status_code_high_rounded, 0.95) AS status_code_high_rounded_p95,
    MAX(status_code_high_rank) AS max_status_code_high_rank,
    SUM(status_code_high_rounded) AS total_status_code_high_rounded
FROM scored
GROUP BY hour_window
