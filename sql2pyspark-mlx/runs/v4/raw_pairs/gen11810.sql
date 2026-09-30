SELECT id, host, level, latency_ms, status_code, attr_key, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
