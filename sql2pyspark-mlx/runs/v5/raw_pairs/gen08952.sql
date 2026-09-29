WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN amount >= 1790 THEN 'High' WHEN amount >= 520 THEN 'Medium' ELSE 'Low' END AS amount_level
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IS DISTINCT FROM 'cash'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        paid_at,
        status,
        CONCAT_WS('-', method, status) AS method_status_key
    FROM base
    WHERE method IS DISTINCT FROM 'cash'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        paid_at,
        status,
        burst_id,
        COUNT(burst_id) OVER (PARTITION BY method) AS method_count_burst_id,
        SUM(burst_id) OVER (PARTITION BY method ORDER BY burst_id ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_burst_id
    FROM sessions_payments
),
base_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        status,
        burst_id,
        method_count_burst_id,
        TO_DATE(paid_at) AS paid_at_date,
        cumulative_burst_id - burst_id AS cumulative_burst_id_minus_burst_id,
        COALESCE(method, 'N/A') AS resolved_method
    FROM enriched
    WHERE status = 'FAILED'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_burst_id_minus_burst_id >= 70 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_burst_id_minus_burst_id_tier,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    COUNT(DISTINCT method) AS distinct_methods
FROM base_payments
GROUP BY 1
ORDER BY distinct_methods DESC
