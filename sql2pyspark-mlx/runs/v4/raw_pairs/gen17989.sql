SELECT
    id,
    service,
    level,
    latency_ms,
    status_code,
    context,
    COALESCE(context['trace'], 'unknown') AS trace_or_default,
    SIZE(context) AS num_context,
    COALESCE(context['version'], 'direct') AS version_or_default,
    context['version'] AS version,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
FROM app_logs
