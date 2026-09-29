WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        service,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM exploded
    WHERE service NOT IN ('checkout', 'billing', 'search')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(attr_key, 'Unknown') attr_key_filled
    FROM parsed
    WHERE attr_key IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(status_code_high AS DOUBLE) status_code_high_num,
        COALESCE(attr_key, 'OTHER') resolved_attr_key
    FROM filtered
    WHERE attr_key IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_high >= 2 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_tier,
    attr_key,
    service,
    MAX(status_code) AS max_status_code,
    MIN(has_fault) AS min_has_fault,
    APPROX_PERCENTILE(status_code_high, 0.9) AS status_code_high_p90,
    AVG(has_fault) FILTER (WHERE attr_key = 'trace') AS trace_avg_has_fault,
    COUNT(CASE WHEN service = 'checkout' THEN 1 END) AS checkout_count,
    MAX(status_code) AS max_status_code_2
FROM valid
GROUP BY status_code_high_tier, attr_key, service;
