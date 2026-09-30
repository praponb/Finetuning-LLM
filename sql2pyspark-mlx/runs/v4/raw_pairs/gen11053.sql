SELECT
    id,
    level,
    status_code,
    context['version'] AS version,
    SIZE(context) AS num_context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
FROM app_logs
WHERE status_code BETWEEN 283 AND 356
