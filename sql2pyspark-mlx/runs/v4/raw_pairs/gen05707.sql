WITH lines AS (
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
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        service,
        level,
        latency_ms,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged
    FROM lines
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        latency_ms,
        v,
        status_code_high,
        is_flagged,
        latency_ms * is_flagged AS latency_ms_times_is_flagged
    FROM flattened_attrs
    WHERE level = 'DEBUG'
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN is_flagged >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_flagged_tier,
        CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_tier,
        MAX(latency_ms) FILTER (WHERE service = 'checkout') AS checkout_max_latency_ms,
        SORT_ARRAY(COLLECT_SET(service)) AS service_list,
        SIZE(COLLECT_SET(service)) AS num_services,
        GROUPING(CASE WHEN is_flagged >= 1 THEN 'HIGH' ELSE 'LOW' END) AS gid
    FROM base
    GROUP BY ROLLUP (CASE WHEN is_flagged >= 1 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING num_services < 1
)
SELECT
    is_flagged_tier,
    status_code_high_tier,
    checkout_max_latency_ms,
    service_list,
    num_services,
    gid
FROM grouped
ORDER BY status_code_high_tier DESC;
