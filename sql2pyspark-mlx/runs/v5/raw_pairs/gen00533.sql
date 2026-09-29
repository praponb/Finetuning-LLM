WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms NULLS FIRST, id, attr_key) AS dedup_rank
        FROM exploded
    ) x
    WHERE dedup_rank = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged_2
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM sessionized
    EXCEPT
    SELECT id
    FROM app_logs
    WHERE service = 'auth'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    service,
    host,
    log_ts,
    value,
    is_new_burst,
    session_start_flag,
    CASE WHEN service RLIKE '^(search|auth)$' THEN 'CORE' WHEN service RLIKE '^(billing|checkout)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS service_zone
FROM verified
ORDER BY log_ts DESC, service DESC
