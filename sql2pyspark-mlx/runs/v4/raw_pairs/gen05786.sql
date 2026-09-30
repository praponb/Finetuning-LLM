WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
)
SELECT
    service,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MIN(log_ts) AS first_log_ts,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    COUNT_IF(level = 'INFO') AS info_events,
    MAX(log_ts) AS last_seen,
    COUNT_IF(attr_key = 'version') AS version_events
FROM lines
GROUP BY service, DATE_TRUNC('HOUR', log_ts)
HAVING (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 >= 0
ORDER BY service;
