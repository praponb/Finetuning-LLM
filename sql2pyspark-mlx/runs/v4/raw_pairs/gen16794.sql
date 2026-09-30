SELECT
    id,
    service,
    host,
    log_ts,
    level,
    payload_json,
    context,
    context['version'] AS version,
    context['region'] AS region,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    CAST((status_code & 1) != 0 AS INT) AS flag_bit1
FROM app_logs
