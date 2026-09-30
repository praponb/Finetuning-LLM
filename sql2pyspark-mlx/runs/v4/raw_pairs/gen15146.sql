WITH exploded AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
)
SELECT
    *,
    LAST_VALUE(attr_key, TRUE) OVER (PARTITION BY attr_key, level ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
    COUNT(status_code) OVER (PARTITION BY level, host) AS level_count_status_code,
    LAST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY level ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key_2
FROM exploded
