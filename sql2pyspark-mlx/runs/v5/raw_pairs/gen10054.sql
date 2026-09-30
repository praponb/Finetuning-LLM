WITH lines AS (SELECT id, host, level, latency_ms, k, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value) SELECT latency_ms FROM lines ORDER BY latency_ms DESC
