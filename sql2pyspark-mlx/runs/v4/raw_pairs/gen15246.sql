WITH top_order_items AS (
    SELECT
        id,
        product_id,
        quantity,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS row_num
    FROM order_items
    WHERE quantity > 5
)
SELECT id, product_id, quantity
FROM top_order_items
WHERE row_num = 1
