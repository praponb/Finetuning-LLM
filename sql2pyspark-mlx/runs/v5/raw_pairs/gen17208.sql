WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label,
        UPPER(method) AS method_upper
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-10-03 13:00:00'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        paid_at_label,
        UPPER(method) AS method_upper_2,
        CAST(paid_at AS DATE) AS paid_at_day,
        COALESCE(method, 'NONE') AS resolved_method
    FROM base
    WHERE paid_at > DATE '2023-11-17'
      AND order_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM cleaned
    EXCEPT ALL
    SELECT order_id, status
    FROM payments
    WHERE status = 'SUCCESS'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.status = k.status
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        method,
        paid_at,
        status,
        paid_at_label,
        method_upper_2,
        paid_at_day,
        resolved_method,
        CONCAT(status, ' ', method) AS status_method,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM verified
    WHERE method IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    method,
    status,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(paid_at) AS ended_at,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list_2,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    GROUPING(method) AS gid
FROM cleaned_payments
GROUP BY CUBE (method, status)
ORDER BY refunded_rows DESC NULLS FIRST
