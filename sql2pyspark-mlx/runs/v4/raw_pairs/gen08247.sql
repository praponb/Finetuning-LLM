SELECT id, service, host, latency_ms, attr_key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
