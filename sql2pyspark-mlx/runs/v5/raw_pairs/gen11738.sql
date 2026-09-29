WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        status_code,
        k,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        k,
        v,
        status_code_high,
        has_fault
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY has_fault DESC NULLS LAST, id, k) AS rank_in_group
        FROM flattened_attrs
    ) t
    WHERE rank_in_group = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        k,
        v,
        status_code_high,
        has_fault,
        CAST(has_fault AS STRING) AS has_fault_num,
        ROUND(has_fault * 0.85, 2) AS has_fault_scaled
    FROM deduped
    WHERE k IS NOT NULL
      AND k IS NOT DISTINCT FROM 'version'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END AS has_fault_tier,
        APPROX_PERCENTILE(has_fault, 0.95) AS has_fault_p95,
        PERCENTILE_APPROX(has_fault_scaled, ARRAY(0.25, 0.5, 0.75)) AS has_fault_scaled_quartiles
    FROM cleaned
    GROUP BY 1
    HAVING has_fault_p95 > 0
)
SELECT has_fault_tier, has_fault_scaled_quartiles
FROM totals;
