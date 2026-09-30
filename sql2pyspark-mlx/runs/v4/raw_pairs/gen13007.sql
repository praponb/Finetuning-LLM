WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, value
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM lines
    INTERSECT
    SELECT id
    FROM carts
    WHERE SIZE(coupon_codes) >= 1
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(key) AS key_lower
    FROM verified
    WHERE created_at >= '2023-04-05'
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    MAX(created_at) AS ended_at,
    COUNT_IF(key = 'weight_kg') AS weight_kg_events,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list
FROM valid
GROUP BY customer_id
ORDER BY customer_id DESC NULLS LAST;
