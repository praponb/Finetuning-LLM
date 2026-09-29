SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    p.product_name,
    o.store_id,
    o.status
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
