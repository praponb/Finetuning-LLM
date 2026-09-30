SELECT id, log_ts, latency_ms, status_code, k, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) li AS k, attr_value
