WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts ASC, id ASC, key ASC) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM exploded
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        SUM(session_start_flag) OVER (PARTITION BY host ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY key ORDER BY island_id DESC, id ASC, key ASC) AS island_id_bucket,
        AVG(island_id) OVER (PARTITION BY level, host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS island_id_avg_2h
    FROM islands
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT host, id
    FROM with_windows
    INTERSECT ALL
    SELECT host, id
    FROM app_logs
    WHERE status_code BETWEEN 261 AND 328
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.host = k.host AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS island_id_bucket_tier,
    COUNT(CASE WHEN key = 'version' THEN 1 END) AS version_count,
    MAX(island_id_avg_2h) AS max_island_id_avg_2h
FROM verified
GROUP BY CASE WHEN island_id_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(CASE WHEN key = 'version' THEN 1 END) < 1000;
