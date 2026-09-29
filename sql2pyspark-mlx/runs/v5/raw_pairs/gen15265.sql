SELECT id, service, host, log_ts, latency_ms, status_code, context, CAST((status_code & 4) != 0 AS INT) AS has_fault, GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id FROM app_logs;
