WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
)
SELECT
    *,
    LEAD(status_code, 1) OVER (ORDER BY log_ts, id, attr_key) AS next_status_code,
    COUNT(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_count_15m
FROM exploded
