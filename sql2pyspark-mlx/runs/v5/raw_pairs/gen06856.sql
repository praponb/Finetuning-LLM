SELECT id, log_ts, level, latency_ms, attr_key, v FROM app_logs LATERAL VIEW EXPLODE(context) x AS attr_key, v
