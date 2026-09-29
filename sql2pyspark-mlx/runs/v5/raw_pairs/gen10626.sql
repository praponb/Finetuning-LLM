WITH lines AS (
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
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(latency_ms, 1) OVER (ORDER BY latency_ms, id, k) AS next_latency_ms,
        NTILE(4) OVER (ORDER BY status_code DESC, id, k) AS status_code_bucket
    FROM lines
    WHERE status_code > 271
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        status_code_bucket,
        SUM(session_start_flag) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level, k ORDER BY log_ts DESC, id, k) = 1
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('DAY', log_ts) AS activity_day,
        COUNT(*) AS row_count,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
        SIZE(COLLECT_SET(level)) AS num_levels,
        COUNT(DISTINCT k) AS distinct_ks,
        COUNT(DISTINCT k) AS distinct_ks_2
    FROM deduped
    GROUP BY DATE_TRUNC('DAY', log_ts)
)
SELECT
    activity_day,
    row_count,
    num_levels,
    distinct_ks,
    distinct_ks_2
FROM totals
ORDER BY activity_day DESC, num_levels DESC
LIMIT 5
