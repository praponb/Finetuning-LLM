SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    k,
    v
FROM app_logs
LATERAL VIEW EXPLODE(context) x AS k, v;
