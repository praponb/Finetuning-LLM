WITH flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        payload_json,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        context['region'] AS region
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 8 AND 14
)
SELECT
    id,
    service,
    log_ts,
    level,
    status_code,
    has_version,
    region,
    MIN(flag_bit8) OVER (PARTITION BY level ORDER BY flag_bit8, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_flag_bit8,
    AVG(flag_bit8) OVER (PARTITION BY level ORDER BY flag_bit8, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_flag_bit8,
    COUNT(flag_bit8) OVER (PARTITION BY level ORDER BY flag_bit8, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_flag_bit8
FROM flattened_attrs
WHERE flag_bit8 IS NOT NULL
