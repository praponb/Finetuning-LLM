SELECT
    id,
    service,
    log_ts,
    payload_json,
    context['region'] AS region,
    context['trace'] AS trace
FROM app_logs;
