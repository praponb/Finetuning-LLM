WITH parsed AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        context,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
    WHERE status_code > 364
)
SELECT
    *,
    RANK() OVER (PARTITION BY level ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank,
    NTILE(3) OVER (PARTITION BY level ORDER BY retry_count, id) AS retry_count_bucket,
    NTILE(5) OVER (ORDER BY has_fault DESC, id) AS has_fault_bucket,
    COUNT(status_code) OVER w_part AS level_count_status_code
FROM parsed
WHERE level = 'INFO'
WINDOW w_part AS (PARTITION BY level);
