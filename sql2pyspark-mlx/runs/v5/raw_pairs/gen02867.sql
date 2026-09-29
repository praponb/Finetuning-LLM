SELECT
    o.id,
    o.order_date,
    o.customer_id,
    o.channel
FROM orders o
WHERE EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id)
