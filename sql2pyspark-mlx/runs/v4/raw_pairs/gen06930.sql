WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(created_at) AS created_at_hour,
        MONTH(created_at) AS created_at_month
    FROM flattened
    WHERE created_at > TIMESTAMP '2025-03-01 17:00:00'
      AND value IS NOT NULL
      AND created_at < '2024-11-01'
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM valid
    EXCEPT ALL
    SELECT customer_id, id
    FROM carts
    WHERE created_at < '2024-01-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        COALESCE(value, 'NONE') AS resolved_value
    FROM verified
    WHERE created_at >= TIMESTAMP '2025-03-02 12:00:00'
      AND value IS NOT NULL
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    SORT_ARRAY(COLLECT_SET(value)) AS value_list,
    AVG(created_at_month) FILTER (WHERE value = 'BUNDLE') AS bundle_avg_created_at_month
FROM valid_carts
GROUP BY 1
