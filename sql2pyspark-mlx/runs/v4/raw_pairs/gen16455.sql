WITH flattened AS (SELECT id, service, level, latency_ms, attr_key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value) SELECT latency_ms, attr_key, value FROM flattened
