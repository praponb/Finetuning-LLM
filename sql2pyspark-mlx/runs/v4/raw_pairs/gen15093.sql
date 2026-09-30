SELECT
    id,
    latency_ms,
    status_code,
    k,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) li AS k, v
