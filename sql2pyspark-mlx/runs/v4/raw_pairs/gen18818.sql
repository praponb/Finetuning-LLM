WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
unpacked AS (
    SELECT
        id,
        log_ts,
        level,
        key,
        value,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
flagged AS (
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM unpacked
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
cleaned AS (
    SELECT
        *,
        flag_bit4 * is_new_burst AS flag_bit4_times_is_new_burst,
        COALESCE(key, 'N/A') AS resolved_key
    FROM islands
    WHERE key IS NOT NULL
)
SELECT
    id,
    log_ts,
    key,
    value,
    status_code_high,
    is_new_burst,
    flag_bit4_times_is_new_burst,
    resolved_key
FROM cleaned
ORDER BY id;
