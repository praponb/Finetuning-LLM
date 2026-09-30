WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_code) OVER w_running AS last3_max_status_code,
        MIN(status_code) OVER w_running AS last3_min_status_code,
        AVG(status_code) OVER w_running AS last3_avg_status_code
    FROM lines
    WHERE host IN ('h3', 'h2')
    WINDOW w_running AS (PARTITION BY host, level ORDER BY latency_ms, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) AS prev_log_ts
    FROM enriched
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v,
        last3_max_status_code,
        last3_min_status_code,
        SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
islands_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        status_code,
        key,
        v,
        last3_max_status_code,
        last3_min_status_code,
        session_id,
        SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        status_code,
        key,
        v,
        last3_max_status_code,
        burst_id,
        COUNT(last3_max_status_code) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last3_max_status_code,
        MAX(last3_max_status_code) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last3_max_status_code,
        AVG(last3_max_status_code) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last3_max_status_code
    FROM islands_app_logs
)
SELECT
    id,
    key,
    v,
    burst_id,
    running_count_last3_max_status_code,
    running_max_last3_max_status_code,
    running_avg_last3_max_status_code,
    CASE WHEN key RLIKE '^(version|trace)$' THEN 'PRIORITY' WHEN key RLIKE '^(region)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS key_zone
FROM metrics
ORDER BY id DESC NULLS LAST;
