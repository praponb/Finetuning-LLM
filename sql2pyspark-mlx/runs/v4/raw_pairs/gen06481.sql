SELECT id, service, host, log_ts, latency_ms, key, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) x AS key, v;
