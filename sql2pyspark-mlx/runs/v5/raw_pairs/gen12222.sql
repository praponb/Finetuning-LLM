SELECT
    oi.order_id,
    o.status,
    o.customer_id,
    s.region
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
INNER JOIN stores s ON o.store_id = s.id
WHERE oi.unit_price BETWEEN 110 AND 330 AND oi.order_id IN (33, 38)
ORDER BY status, order_id DESC;
