SELECT id, service, log_ts, latency_ms, status_code, k, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) x AS k, v;
