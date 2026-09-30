WITH exploded AS (SELECT id, latency_ms, status_code, k, value FROM app_logs LATERAL VIEW EXPLODE(context) li AS k, value) SELECT id, status_code, k FROM exploded;
