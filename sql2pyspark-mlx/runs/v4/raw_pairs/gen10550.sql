WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM unpacked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        k,
        has_fault,
        ROUND(status_code_high, 0) AS status_code_high_rounded
    FROM islands
    WHERE log_ts >= TIMESTAMP '2025-06-01 17:00:00'
      AND k IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) AS prev_log_ts
    FROM filtered
),
gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY is_new_session DESC NULLS LAST) AS dedup_rank
        FROM sessionized
    ) x
    WHERE dedup_rank = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', prev_log_ts) AS year_start,
    session_no,
    CASE WHEN status_code_high_rounded >= 73 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_rounded_tier,
    PERCENTILE_APPROX(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles,
    COUNT(CASE WHEN k = 'trace' THEN 1 END) AS trace_count,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM latest
GROUP BY DATE_TRUNC('YEAR', prev_log_ts), session_no, CASE WHEN status_code_high_rounded >= 73 THEN 'HIGH' ELSE 'LOW' END
ORDER BY year_start DESC NULLS FIRST
