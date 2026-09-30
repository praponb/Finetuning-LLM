WITH with_latest AS (
    SELECT s.*, x.order_item_quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    launch_date,
    tags
FROM with_latest
ORDER BY category DESC
