SELECT id, service, host, log_ts, latency_ms, status_code, payload_json, context, GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text, context['region'] AS region FROM app_logs;
