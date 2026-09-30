SELECT
    id,
    service,
    host,
    log_ts,
    level,
    latency_ms,
    status_code,
    payload_json,
    context,
    ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region,
    COALESCE(context['trace'], 'direct') AS trace_or_default,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
    COALESCE(context['region'], 'direct') AS region_or_default,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
FROM app_logs
