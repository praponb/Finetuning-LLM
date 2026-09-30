SELECT
    id,
    log_ts,
    latency_ms,
    payload_json,
    context,
    context['region'] AS region,
    COALESCE(context['region'], 'unknown') AS region_or_default
FROM app_logs
