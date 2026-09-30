WITH top_order_items AS (
    SELECT
        id,
        order_id,
        quantity,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS row_num
    FROM order_items
    WHERE quantity >= 7
)
SELECT
    id,
    order_id,
    quantity,
    product_id
FROM top_order_items
WHERE row_num <= 5
