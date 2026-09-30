WITH base AS (
    SELECT *
    FROM order_items
    WHERE product_id IS NULL
),
summary AS (
    SELECT product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, MIN(quantity) AS min_quantity
    FROM base
    GROUP BY product_id
)
SELECT product_id, min_quantity
FROM summary
ORDER BY min_quantity DESC, product_id
LIMIT 10;
