WITH lines AS (SELECT id, log_ts, level, latency_ms, key, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) li AS key, attr_value) SELECT id, latency_ms, key, attr_value FROM lines ORDER BY key;
