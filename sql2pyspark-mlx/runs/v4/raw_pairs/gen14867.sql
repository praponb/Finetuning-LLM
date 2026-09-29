WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS row_seq,
        LAST_VALUE(key, TRUE) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_key
    FROM flattened
    WINDOW w_recent AS (PARTITION BY key ORDER BY log_ts DESC, id, key)
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) AS prev_log_ts
    FROM metrics
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        island_id,
        COUNT_IF(key = 'region') AS region_events,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list,
        ROUND(AVG(row_seq), 2) AS avg_row_seq,
        ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS row_seq_per_trace,
        ROUND(AVG(row_seq), 2) AS avg_row_seq_2,
        SIZE(COLLECT_SET(key)) AS num_keys
    FROM sessions_app_logs
    GROUP BY hour_window, island_id
)
SELECT
    hour_window,
    island_id,
    key_list,
    row_seq_per_trace,
    avg_row_seq_2
FROM totals
ORDER BY row_seq_per_trace NULLS LAST;
