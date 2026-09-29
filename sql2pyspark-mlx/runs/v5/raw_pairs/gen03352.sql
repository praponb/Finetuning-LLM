WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(attr_key) AS attr_key_length
    FROM lines
    WHERE log_ts >= TIMESTAMP '2025-06-01 09:00:00'
      AND attr_key IS DISTINCT FROM 'region'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key) AS prev_log_ts
    FROM filtered
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        v,
        prev_log_ts,
        session_start_flag,
        island_id,
        AVG(island_id) OVER (PARTITION BY attr_key ORDER BY attr_key_length, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_island_id,
        MIN(island_id) OVER (PARTITION BY attr_key ORDER BY attr_key_length, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_island_id,
        COUNT(island_id) OVER (PARTITION BY attr_key ORDER BY attr_key_length, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_island_id,
        FIRST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY level ORDER BY prev_log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms
    FROM islands
    WHERE service = 'auth'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_log_ts) OVER (PARTITION BY attr_key ORDER BY prev_log_ts, id, attr_key) IS NULL
                 OR prev_log_ts > LAG(prev_log_ts) OVER (PARTITION BY attr_key ORDER BY prev_log_ts, id, attr_key) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
islands_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY attr_key ORDER BY prev_log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        prev_log_ts,
        island_id,
        last3_avg_island_id,
        last3_min_island_id,
        last3_count_island_id,
        first_known_latency_ms,
        is_new_burst,
        burst_id,
        ROW_NUMBER() OVER (PARTITION BY attr_key, level ORDER BY island_id DESC, id, attr_key) AS row_seq,
        COUNT(first_known_latency_ms) OVER (PARTITION BY level ORDER BY CAST(prev_log_ts AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS first_known_latency_ms_count_90d
    FROM islands_app_logs
)
SELECT
    id,
    level,
    attr_key,
    last3_count_island_id,
    is_new_burst,
    row_seq,
    first_known_latency_ms_count_90d,
    CASE WHEN attr_key RLIKE '^(trace|version)$' THEN 'GROUP_B' WHEN attr_key RLIKE '^(region)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS attr_key_zone
FROM enriched
