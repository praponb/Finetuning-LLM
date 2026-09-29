WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, quantity + unit_price AS line_total
    FROM order_items
    WHERE quantity BETWEEN 6 AND 7
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.quantity,
        src.unit_price,
        o.amount
    FROM base src
    INNER JOIN orders o
        ON src.order_id = o.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM enriched
    EXCEPT
    SELECT order_id
    FROM order_items
    WHERE quantity < 5
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.order_id,
        e.unit_price,
        o.status,
        o.discount
    FROM reconciled e
    INNER JOIN orders o
        ON e.order_id = o.id
    WHERE o.amount BETWEEN 1110 AND 1550
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN discount >= 0.37 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
        status,
        MIN(discount) AS min_discount,
        SUM(discount) AS total_discount,
        COUNT(DISTINCT status) AS distinct_status,
        PERCENTILE_APPROX(discount, 0.5) AS discount_p50,
        COUNT(*) AS row_count,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM joined
    GROUP BY discount_tier, status
)
SELECT
    discount_tier,
    status,
    min_discount,
    total_discount,
    distinct_status,
    discount_p50,
    status_list
FROM summary
ORDER BY distinct_status NULLS FIRST
