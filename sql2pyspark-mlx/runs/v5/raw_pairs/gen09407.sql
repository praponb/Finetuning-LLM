WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM flattened
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        SUM(session_start_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessions_app_logs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts DESC, id, key) = 1
),
ranked_app_logs AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code DESC, id, key) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM ranked_app_logs
    INTERSECT
    SELECT id
    FROM app_logs
    WHERE latency_ms <= 2272
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked_app_logs s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        log_ts,
        status_code,
        key,
        island_id,
        status_code * latency_ms AS status_code_times_latency_ms,
        COALESCE(key, 'UNKNOWN') AS resolved_key
    FROM reconciled
    WHERE log_ts >= TIMESTAMP '2025-06-01 09:00:00'
)
SELECT
    status_code,
    island_id,
    resolved_key
FROM valid
ORDER BY status_code DESC NULLS FIRST, island_id;
