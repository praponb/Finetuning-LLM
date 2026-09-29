SELECT
    id,
    host,
    latency_ms,
    COALESCE(context['region'], 'unknown') AS region_or_default,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region,
    context['region'] AS region
FROM app_logs;
