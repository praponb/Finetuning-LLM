WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
)
-- Grouped metrics
SELECT
    key,
    BIT_AND(latency_ms) AS latency_ms_bit_and,
    APPROX_PERCENTILE(latency_ms, 0.5) AS latency_ms_p50
FROM lines
GROUP BY key
HAVING APPROX_PERCENTILE(latency_ms, 0.5) < 0
ORDER BY latency_ms_bit_and NULLS FIRST;
