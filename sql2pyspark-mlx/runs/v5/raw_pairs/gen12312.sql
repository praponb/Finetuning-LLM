WITH matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM order_items
    INTERSECT
    SELECT id
    FROM order_items
    WHERE unit_price BETWEEN 110 AND 250
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.order_id,
        s.product_id,
        b.order_date,
        b.discount
    FROM kept_rows s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.amount,
        d.discount AS order_discount
    FROM enriched e
    JOIN orders d
        ON e.order_id = d.id
    WHERE d.shipping_fee <= 31
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, p.status
    FROM joined src
    JOIN orders p
        ON src.order_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN order_discount >= 0.35 THEN 'HIGH' ELSE 'LOW' END AS order_discount_tier,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    SUM(discount) AS total_discount,
    MAX(discount) FILTER (WHERE status = 'CANCELLED') AS cancelled_max_discount,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END), 0), 2) AS amount_per_completed,
    COUNT(*) FILTER (WHERE status = 'RETURNED') AS returned_rows
FROM with_dims
GROUP BY order_discount_tier, week_start;
