WITH flattened AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
)
SELECT id
FROM flattened
ORDER BY id NULLS FIRST;
