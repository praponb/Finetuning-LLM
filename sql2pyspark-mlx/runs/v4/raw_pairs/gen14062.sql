SELECT id, host, log_ts, level, latency_ms, status_code, attr_key, value FROM app_logs LATERAL VIEW EXPLODE(context) e AS attr_key, value;
