SELECT id, service, host, level, latency_ms, k, value FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, value;
