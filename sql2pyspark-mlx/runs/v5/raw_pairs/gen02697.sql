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
    LATERAL VIEW EXPLODE(context) t AS k, v
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        status_code,
        k,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM unpacked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        k,
        has_fault,
        status_code_high,
        SUM(new_group_flag) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(status_code_high, 1) OVER (PARTITION BY k ORDER BY has_fault, id, k) next_status_code_high,
        MAX(has_fault) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) has_fault_max_15m,
        LAST_VALUE(has_fault, TRUE) OVER (ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_has_fault,
        SUM(has_fault) OVER (PARTITION BY k ORDER BY status_code_high, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_has_fault,
        LAST_VALUE(status_code_high, TRUE) OVER (PARTITION BY k ORDER BY has_fault, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_status_code_high
    FROM islands
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(running_has_fault) OVER w_hist prev_running_has_fault,
        LAG(last_known_has_fault) OVER w_hist prev_last_known_has_fault,
        LEAD(last_known_has_fault) OVER w_hist next_last_known_has_fault,
        ROW_NUMBER() OVER w_hist row_seq,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY next_status_code_high DESC) next_status_code_high_percent_rank
    FROM metrics
    WINDOW w_hist AS (PARTITION BY k ORDER BY has_fault, id, k)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT
    SELECT id
    FROM app_logs
    WHERE host NOT IN ('h2', 'h1')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    CASE WHEN next_status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_status_code_high_tier,
    COUNT(CASE WHEN k = 'trace' THEN 1 END) AS trace_count,
    ROUND(SUM(has_fault_max_15m) / NULLIF(COUNT(CASE WHEN k = 'trace' THEN 1 END), 0), 2) AS has_fault_max_15m_per_trace,
    AVG(last_known_has_fault) AS avg_last_known_has_fault
FROM kept_rows
GROUP BY DATE_TRUNC('DAY', log_ts), CASE WHEN next_status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY trace_count;
