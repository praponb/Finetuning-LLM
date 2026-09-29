WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        latency_ms,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM exploded
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(service) AS service_length,
        COALESCE(attr_key, 'Unknown') AS attr_key_filled
    FROM parsed
    WHERE attr_key IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(has_fault, status_code_high) AS least_has_fault_status_code_high,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM base
    WHERE attr_key IS NOT NULL
)
-- Grouped metrics
SELECT
    service,
    SUM(CASE WHEN service = 'checkout' THEN latency_ms ELSE 0 END) AS checkout_latency_ms,
    COUNT(*) FILTER (WHERE attr_key = 'version') AS version_rows,
    APPROX_PERCENTILE(service_length, 0.95) AS service_length_p95
FROM filtered
GROUP BY CUBE (service)
ORDER BY checkout_latency_ms DESC;
