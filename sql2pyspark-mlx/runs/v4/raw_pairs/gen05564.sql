WITH ranked AS (
    SELECT
        id,
        product_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rn
    FROM order_items
)
SELECT id, product_id, quantity
FROM ranked
WHERE rn <= 5;
