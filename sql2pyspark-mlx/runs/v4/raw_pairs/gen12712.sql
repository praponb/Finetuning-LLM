SELECT
    id,
    log_ts,
    level,
    latency_ms,
    status_code,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    context['version'] AS version,
    SHIFTRIGHT(status_code, 2) AS status_code_high
FROM app_logs;
