WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(latency_ms) OVER (PARTITION BY level) AS level_min_latency_ms,
        LEAD(latency_ms, 2, 0) OVER (PARTITION BY level ORDER BY latency_ms, id, key) AS next_latency_ms,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY latency_ms) AS latency_ms_dense_rank,
        LEAD(latency_ms, 2, 0) OVER (ORDER BY log_ts, id, key) AS next_latency_ms_2,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share
    FROM exploded
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM sessions_app_logs
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY is_new_session NULLS FIRST, id, key) AS rank_in_group
        FROM sessionized
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY key, level ORDER BY status_code_share, id, key) AS event_seq,
        COUNT(next_latency_ms_2) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS next_latency_ms_2_count_30m,
        CUME_DIST() OVER (PARTITION BY level ORDER BY is_new_session) AS is_new_session_cume_dist
    FROM deduped
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) AS latency_ms_share,
        LEAD(session_id, 1, 0) OVER (PARTITION BY level ORDER BY status_code_share, id, key) AS next_session_id,
        ROW_NUMBER() OVER (PARTITION BY key, level ORDER BY new_group_flag, id, key) AS event_seq_2
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN latency_ms_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_share_tier,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE level = 'INFO') AS info_rows,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT_IF(level = 'ERROR') AS error_events,
    APPROX_PERCENTILE(new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS new_group_flag_quartiles
FROM metrics
GROUP BY CASE WHEN latency_ms_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY num_levels DESC;
