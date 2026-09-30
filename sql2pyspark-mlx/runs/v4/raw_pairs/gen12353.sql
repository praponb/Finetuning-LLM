SELECT
    id,
    service,
    log_ts,
    latency_ms,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
    context['version'] AS version
FROM app_logs
