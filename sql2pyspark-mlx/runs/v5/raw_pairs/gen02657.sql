SELECT
    id,
    service,
    host,
    log_ts,
    latency_ms,
    payload_json,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    COALESCE(context['trace'], 'direct') AS trace_or_default
FROM app_logs
WHERE TO_DATE(log_ts) < '2023-06-01'
