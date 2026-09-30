WITH lines AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value
)
SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_percent_rank,
    CORR(status_code, latency_ms) OVER (PARTITION BY host, attr_key) AS corr_status_code_latency_ms,
    CORR(latency_ms, status_code) OVER (PARTITION BY service, attr_key) AS corr_latency_ms_status_code
FROM lines;
