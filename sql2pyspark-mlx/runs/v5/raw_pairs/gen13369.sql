SELECT
    s.id,
    s.order_id,
    s.status,
    s.delivered_date,
    o.discount
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE s.status NOT IN ('RETURNED', 'DELIVERED', 'LOST');
