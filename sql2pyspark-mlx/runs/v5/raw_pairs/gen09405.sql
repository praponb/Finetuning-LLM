SELECT
    id,
    service,
    host,
    log_ts,
    latency_ms,
    payload_json,
    context,
    COALESCE(context['region'], 'none') AS region_or_default,
    context['region'] AS region
FROM app_logs;
