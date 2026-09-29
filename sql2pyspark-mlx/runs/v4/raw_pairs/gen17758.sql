WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN latency_ms >= 2609 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_tier,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    SUM(CASE WHEN service = 'billing' THEN status_code ELSE 0 END) AS billing_status_code,
    COUNT_IF(service = 'billing') AS billing_events
FROM exploded
GROUP BY ROLLUP (CASE WHEN latency_ms >= 2609 THEN 'HIGH' ELSE 'LOW' END);
