WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        status_code,
        k,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM flattened
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        k,
        attr_value,
        status_code_high,
        has_fault,
        has_fault / NULLIF(status_code_high, 0) AS has_fault_per_status_code_high
    FROM flattened_attrs
    WHERE status_code = 319
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN k IN ('region', 'trace') THEN 'Group A' ELSE 'Other' END AS k_group,
        COALESCE(k, 'N/A') AS k_clean
    FROM filtered
    WHERE has_fault_per_status_code_high BETWEEN 27 AND 54
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(has_fault * 1.1, 2) AS adjusted_has_fault
    FROM filtered_app_logs
    WHERE has_fault <= 0
)
-- Grouped metrics
SELECT
    CASE WHEN has_fault >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS has_fault_tier,
    k,
    SUM(CASE WHEN k = 'region' THEN adjusted_has_fault ELSE 0 END) AS region_adjusted_has_fault,
    SUM(has_fault) AS total_has_fault,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN has_fault >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN k = 'region' THEN adjusted_has_fault ELSE 0 END) DESC) AS region_adjusted_has_fault_percent_rank
FROM valid
GROUP BY CASE WHEN has_fault >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, k;
