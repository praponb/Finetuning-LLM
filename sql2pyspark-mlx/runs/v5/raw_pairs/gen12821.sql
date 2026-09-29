WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM exploded
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        attr_key,
        SUM(is_new_burst) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        log_ts,
        level,
        attr_key,
        session_id,
        NTILE(4) OVER (PARTITION BY host, service ORDER BY session_id DESC, id, attr_key) AS session_id_bucket,
        MAX(session_id) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS session_id_max_1h
    FROM sessions_app_logs
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
sessions_app_logs_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps_2
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        attr_key,
        session_id_bucket,
        session_id_max_1h,
        new_group_flag,
        session_id_2,
        DATE_TRUNC('year', log_ts) AS log_ts_period,
        ROUND(session_id_2 / session_id_bucket, 2) AS session_id_2_per_session_id_bucket
    FROM sessions_app_logs_2
    WHERE log_ts > TO_DATE('2025-06-01')
      AND level = 'DEBUG'
)
-- Aggregate with conditional metrics
SELECT
    attr_key,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN attr_key = 'trace' THEN 1 END) AS trace_count,
    COUNT(CASE WHEN attr_key = 'region' THEN 1 END) AS region_count
FROM valid
GROUP BY 1
HAVING event_count > 1
ORDER BY event_count DESC
