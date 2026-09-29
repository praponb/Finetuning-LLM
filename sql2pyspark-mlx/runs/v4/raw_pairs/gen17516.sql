WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM flattened
    INTERSECT
    SELECT id, customer_id
    FROM carts
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(element) AS element_lower
    FROM reconciled
    WHERE created_at >= TIMESTAMP '2025-03-02 08:00:00'
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, d.segment
    FROM valid a
    LEFT JOIN customers d
        ON a.customer_id = d.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(*) AS cnt,
    MIN(created_at) AS started_at
FROM joined
GROUP BY DATE_TRUNC('HOUR', created_at)
ORDER BY hour_window NULLS FIRST;
