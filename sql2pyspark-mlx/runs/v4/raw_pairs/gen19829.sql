SELECT id, host, latency_ms, status_code, attr_key, value FROM app_logs LATERAL VIEW EXPLODE(context) x AS attr_key, value;
