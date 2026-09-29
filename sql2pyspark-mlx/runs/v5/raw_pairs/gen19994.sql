SELECT id, service, log_ts, level, latency_ms, status_code, key, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value;
