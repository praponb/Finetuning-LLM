SELECT
    oi.unit_price,
    oi.product_id,
    oi.order_id,
    o.store_id,
    o.status
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
