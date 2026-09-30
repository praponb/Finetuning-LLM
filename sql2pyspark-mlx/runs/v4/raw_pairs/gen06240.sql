SELECT id, service, level, latency_ms, key, value FROM app_logs LATERAL VIEW EXPLODE(context) li AS key, value
