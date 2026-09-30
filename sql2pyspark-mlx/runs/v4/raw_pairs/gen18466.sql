SELECT
    id,
    level,
    latency_ms,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    COALESCE(context['trace'], 'unknown') AS trace_or_default,
    context['region'] AS region
FROM app_logs
