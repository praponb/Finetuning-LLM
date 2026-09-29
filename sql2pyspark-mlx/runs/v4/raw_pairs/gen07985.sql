WITH valid_order_items AS (
    SELECT *, unit_price + quantity AS line_total
    FROM order_items
    WHERE product_id IS NULL
),
Agg AS (
    SELECT product_id, COUNT(DISTINCT line_total) AS unique_products
    FROM valid_order_items
    GROUP BY product_id
)
SELECT product_id, unique_products
FROM Agg
ORDER BY product_id DESC;
