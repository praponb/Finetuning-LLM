WITH lines AS (
    SELECT
        id,
        service,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
)
SELECT
    id,
    service,
    level,
    k,
    STDDEV_SAMP(latency_ms) OVER w_recent AS service_stddev_samp_latency_ms,
    MIN(latency_ms) OVER w_recent AS service_min_latency_ms
FROM lines
WINDOW w_recent AS (PARTITION BY service)
