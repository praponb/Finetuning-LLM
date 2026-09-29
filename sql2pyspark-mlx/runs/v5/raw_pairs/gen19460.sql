SELECT
    id,
    host,
    latency_ms,
    status_code,
    payload_json,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    COALESCE(context['version'], 'unknown') AS version_or_default,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
FROM app_logs
