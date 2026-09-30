WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    attr_key,
    value,
    CORR(latency_ms, status_code) OVER w_hist corr_latency_ms_status_code,
    STDDEV_SAMP(latency_ms) OVER (PARTITION BY service) service_stddev_samp_latency_ms
FROM lines
WINDOW w_hist AS (PARTITION BY host);
