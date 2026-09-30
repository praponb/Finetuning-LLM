WITH ranked_order_items AS (
    SELECT
        id,
        order_id,
        unit_price,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rnk
    FROM order_items
    WHERE product_id IN (8, 19, 25, 38)
)
SELECT
    id,
    order_id,
    unit_price,
    quantity
FROM ranked_order_items
WHERE rnk <= 3
