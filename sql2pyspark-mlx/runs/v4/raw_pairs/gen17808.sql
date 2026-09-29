SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    o.status,
    p.product_name
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
FULL OUTER JOIN products p ON oi.product_id = p.id
