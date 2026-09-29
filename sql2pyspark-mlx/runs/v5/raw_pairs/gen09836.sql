SELECT id, log_ts, level, latency_ms, k, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS k, v;
