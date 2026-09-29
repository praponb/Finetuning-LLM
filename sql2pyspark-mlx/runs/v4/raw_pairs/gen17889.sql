SELECT id, service, host, level, latency_ms, status_code, k, v FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, v;
