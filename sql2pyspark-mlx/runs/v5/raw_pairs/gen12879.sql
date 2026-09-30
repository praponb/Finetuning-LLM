SELECT
    id,
    service,
    level,
    payload_json,
    context,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
FROM app_logs
WHERE service = 'billing'
