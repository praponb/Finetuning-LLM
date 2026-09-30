WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts ASC, id ASC, key ASC) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM lines
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        v,
        is_new_session,
        island_id,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) AS latency_ms_share,
        NTILE(5) OVER (PARTITION BY key ORDER BY latency_ms DESC, id ASC, key ASC) AS latency_ms_bucket
    FROM islands
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(is_new_session, 1, 0) OVER (ORDER BY latency_ms_share ASC, id ASC, key ASC) AS prev_is_new_session,
        LAG(island_id, 1) OVER (PARTITION BY key ORDER BY is_new_session ASC, id ASC, key ASC) AS prev_island_id
    FROM with_windows
)
SELECT
    id,
    log_ts,
    key,
    v,
    island_id,
    latency_ms_share,
    latency_ms_bucket,
    prev_is_new_session,
    prev_island_id
FROM metrics
ORDER BY id DESC NULLS LAST;
