SELECT id, service, log_ts, latency_ms, status_code, key, v FROM app_logs LATERAL VIEW EXPLODE(context) li AS key, v;
