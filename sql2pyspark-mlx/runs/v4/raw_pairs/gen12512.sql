SELECT
    s.id,
    s.status,
    s.delivered_date,
    s.carrier
FROM shipments s
WHERE s.cost BETWEEN 31 AND 42 AND s.order_id IN (SELECT id FROM orders WHERE channel IN ('mobile', 'store'))
