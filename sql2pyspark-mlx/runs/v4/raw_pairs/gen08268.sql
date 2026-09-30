SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    context,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    context['version'] AS version,
    COALESCE(context['trace'], 'none') AS trace_or_default,
    SIZE(context) AS num_context
FROM app_logs
WHERE host IN ('h2', 'h3');
