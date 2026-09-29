WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM flattened
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        service,
        log_ts,
        k,
        v,
        SUM(is_new_burst) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(burst_id, 0) AS burst_id_rounded
    FROM sessions_app_logs
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, service
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, service
    FROM app_logs
    WHERE status_code BETWEEN 346 AND 416
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.service = k.service
)
SELECT
    id,
    service,
    k,
    v,
    is_new_burst,
    burst_id,
    burst_id_rounded
FROM kept_rows
ORDER BY is_new_burst DESC NULLS LAST
