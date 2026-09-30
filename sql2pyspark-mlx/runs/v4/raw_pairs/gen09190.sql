SELECT id, service, log_ts, latency_ms, attr_key, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value;
