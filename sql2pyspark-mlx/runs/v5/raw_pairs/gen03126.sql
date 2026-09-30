WITH base AS (
    SELECT
        id,
        order_id,
        unit_price,
        LEAST(unit_price, quantity) AS least_unit_price_quantity
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NOT NULL
),
kept_keys AS (
    SELECT order_id
    FROM base
    EXCEPT DISTINCT
    SELECT order_id
    FROM order_items
    WHERE order_id = 16
),
kept_rows AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.order_id = k.order_id
),
valid AS (
    SELECT *, unit_price / NULLIF(least_unit_price_quantity, 0) AS unit_price_per_least_unit_price_quantity
    FROM kept_rows
    WHERE least_unit_price_quantity < 80
),
with_dims AS (
    SELECT
        a.*,
        p.amount,
        p.discount,
        p.order_date
    FROM valid a
    LEFT JOIN orders p
        ON a.order_id = p.id
    WHERE p.shipping_fee <= 10
),
metrics AS (
    SELECT *, PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY discount NULLS FIRST) AS discount_percent_rank
    FROM with_dims
),
remaining_keys AS (
    SELECT id, order_id
    FROM metrics
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM order_items
    WHERE product_id IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
)
SELECT
    o.status,
    COUNT(*) AS num_rows,
    SUM(a.unit_price_per_least_unit_price_quantity) AS total_unit_price_per_least_unit_price_quantity
FROM verified a
LEFT JOIN orders o ON a.order_id = o.id
GROUP BY ROLLUP (o.status)
