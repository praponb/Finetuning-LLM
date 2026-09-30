SELECT
    s.id,
    s.carrier,
    o.shipping_fee,
    o.amount,
    o.status
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.shipping_fee < 34
ORDER BY status, id
LIMIT 20;
