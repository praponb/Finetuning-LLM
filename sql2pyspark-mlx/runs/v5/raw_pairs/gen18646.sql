WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, attr_value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE service NOT IN ('auth', 'search', 'billing')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, host), 4) AS latency_ms_share,
        LAST_VALUE(latency_ms, TRUE) OVER (PARTITION BY service, host ORDER BY status_code_high, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms
    FROM parsed
    WHERE key = 'trace'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(status_code_high, has_fault) AS greatest_status_code_high_has_fault
    FROM scored
    WHERE key IS NOT NULL
      AND host = 'h2'
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(has_fault) OVER (PARTITION BY service, key ORDER BY last_known_latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_has_fault,
        SUM(has_fault) OVER (PARTITION BY service, key ORDER BY last_known_latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_has_fault,
        MIN(has_fault) OVER (PARTITION BY service, key ORDER BY last_known_latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_has_fault,
        ROW_NUMBER() OVER (ORDER BY has_fault, id, key) AS row_seq
    FROM filtered
)
-- Multi-level aggregation
SELECT
    service,
    host,
    ROUND(SUM(running_sum_has_fault) / NULLIF(COUNT(CASE WHEN host = 'h4' THEN 1 END), 0), 2) AS running_sum_has_fault_per_h4,
    COUNT_IF(host = 'h1') AS h1_events,
    COUNT(CASE WHEN service = 'checkout' THEN 1 END) AS checkout_count,
    ROUND(SUM(running_avg_has_fault) / NULLIF(COUNT(CASE WHEN host = 'h3' THEN 1 END), 0), 2) AS running_avg_has_fault_per_h3,
    COUNT(CASE WHEN host = 'h4' THEN 1 END) AS h4_count
FROM scored_app_logs
GROUP BY service, host
HAVING COUNT(CASE WHEN host = 'h4' THEN 1 END) > 1
