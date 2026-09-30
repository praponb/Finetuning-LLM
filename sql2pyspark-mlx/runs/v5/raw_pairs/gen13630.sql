WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM flattened
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        key,
        value,
        SUM(session_start_flag) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        log_ts,
        latency_ms,
        key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms DESC NULLS FIRST, id, key) AS rank_in_group
        FROM islands
    ) ranked
    WHERE rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM latest
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE latency_ms IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    log_ts,
    latency_ms,
    key,
    CASE WHEN key RLIKE '^(version|trace)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS key_zone
FROM kept_rows
ORDER BY log_ts;
