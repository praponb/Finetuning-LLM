SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
FROM app_logs
WHERE status_code BETWEEN 369 AND 402
