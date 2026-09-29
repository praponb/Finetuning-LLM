WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(key, TRUE) OVER (ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_key,
        NTILE(4) OVER (ORDER BY latency_ms, id, key) AS latency_ms_bucket,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY latency_ms DESC, id, key) AS row_seq
    FROM lines
),
with_windows_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        key,
        attr_value,
        last_known_key,
        latency_ms_bucket,
        row_seq,
        COUNT(latency_ms_bucket) OVER w_running AS running_count_latency_ms_bucket,
        MAX(latency_ms_bucket) OVER w_running AS running_max_latency_ms_bucket,
        MIN(latency_ms_bucket) OVER w_running AS running_min_latency_ms_bucket
    FROM with_windows
    WINDOW w_running AS (PARTITION BY host ORDER BY latency_ms_bucket, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        key,
        latency_ms_bucket,
        running_count_latency_ms_bucket,
        LAG(running_count_latency_ms_bucket, 2, 0) OVER (ORDER BY latency_ms_bucket, id, key) AS prev_running_count_latency_ms_bucket,
        LAG(row_seq, 1) OVER (PARTITION BY level, key ORDER BY row_seq, id, key) AS prev_row_seq,
        ROUND(running_min_latency_ms_bucket / SUM(running_min_latency_ms_bucket) OVER (PARTITION BY key), 4) AS running_min_latency_ms_bucket_share
    FROM with_windows_app_logs
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY prev_running_count_latency_ms_bucket DESC NULLS FIRST, id, key) = 1
),
with_windows_app_logs_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        key,
        prev_row_seq,
        running_min_latency_ms_bucket_share,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY prev_running_count_latency_ms_bucket NULLS FIRST) AS prev_running_count_latency_ms_bucket_dense_rank,
        LAG(prev_row_seq, 2) OVER (PARTITION BY key ORDER BY prev_row_seq, id, key) AS prev_prev_row_seq
    FROM ranked
)
SELECT
    id,
    key,
    prev_row_seq,
    running_min_latency_ms_bucket_share,
    prev_running_count_latency_ms_bucket_dense_rank,
    prev_prev_row_seq,
    CASE WHEN key RLIKE '^(region|trace)$' THEN 'GROUP_A' WHEN key RLIKE '^(version)$' THEN 'GROUP_B' ELSE 'CORE' END AS key_zone
FROM with_windows_app_logs_2;
