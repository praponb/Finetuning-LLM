WITH base AS (
    SELECT *
    FROM order_items
    WHERE order_id = 38 AND quantity < 3
),
product_id_summary AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, COUNT(quantity) AS quantity_count
    FROM base
    GROUP BY product_id
)
SELECT product_id, quantity_range, quantity_count
FROM product_id_summary;
