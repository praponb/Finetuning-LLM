WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        latency_ms,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE latency_ms = 1597
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN status_code_high >= 3 THEN 'High' WHEN status_code_high >= 1 THEN 'Medium' ELSE 'Low' END AS status_code_high_tier,
        CAST(is_flagged AS STRING) AS is_flagged_str
    FROM flattened_attrs
    WHERE status_code_high <> 2
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN latency_ms >= 1957 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
        CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_tier,
        APPROX_PERCENTILE(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
        PERCENTILE_APPROX(status_code_high, 0.95) AS status_code_high_p95,
        MAX(latency_ms) AS max_latency_ms,
        BIT_AND(latency_ms) AS latency_ms_bit_and,
        BIT_AND(latency_ms) AS latency_ms_bit_and_2
    FROM valid
    GROUP BY 1, 2
)
SELECT
    latency_ms_tier,
    status_code_high_tier,
    latency_ms_quartiles,
    max_latency_ms,
    latency_ms_bit_and_2
FROM grouped
ORDER BY latency_ms_tier DESC NULLS FIRST, status_code_high_tier
