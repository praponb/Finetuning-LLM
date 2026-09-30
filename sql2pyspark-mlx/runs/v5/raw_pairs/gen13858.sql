WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM flattened
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service ORDER BY burst_id DESC, id, key) = 1
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) AS prev_log_ts
    FROM deduped
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY latency_ms DESC NULLS FIRST, id, key) AS row_num
        FROM sessionized
    ) t
    WHERE row_num = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        new_group_flag,
        is_new_session
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host ORDER BY log_ts DESC, id, key) = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        new_group_flag,
        DATE_FORMAT(log_ts, 'MMM yyyy') AS log_ts_label,
        ROUND(status_code * 0.85, 2) AS status_code_scaled,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM latest
    WHERE log_ts > TO_DATE('2025-06-01')
      AND latency_ms BETWEEN 601 AND 1303
)
SELECT
    id,
    host,
    log_ts,
    latency_ms,
    key,
    new_group_flag,
    log_ts_label
FROM cleaned;
