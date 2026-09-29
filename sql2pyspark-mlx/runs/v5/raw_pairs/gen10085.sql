SELECT id, host, log_ts, level, latency_ms, key, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS key, v;
