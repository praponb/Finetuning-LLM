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
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM lines
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY host ORDER BY has_fault DESC NULLS LAST) AS row_num
    FROM flattened_attrs
    QUALIFY row_num = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        attr_key,
        status_code_high,
        has_fault,
        row_num,
        GREATEST(has_fault, status_code) AS greatest_has_fault_status_code,
        CASE WHEN has_fault >= 1 THEN 'High' WHEN has_fault >= 0 THEN 'Medium' ELSE 'Low' END AS has_fault_tier
    FROM ranked
    WHERE row_num <> 1
),
ranked_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host, attr_key ORDER BY row_num DESC, id, attr_key) AS rn
    FROM filtered
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    attr_key,
    CASE WHEN greatest_has_fault_status_code >= 68 THEN 'LARGE' ELSE 'SMALL' END AS greatest_has_fault_status_code_tier,
    CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END AS has_fault_tier,
    COUNT_IF(host = 'h3') AS h3_events,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN host = 'h1' THEN has_fault ELSE 0 END) AS h1_has_fault
FROM ranked_app_logs
GROUP BY GROUPING SETS ((attr_key, CASE WHEN greatest_has_fault_status_code >= 68 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END), (attr_key, CASE WHEN greatest_has_fault_status_code >= 68 THEN 'LARGE' ELSE 'SMALL' END), ());
