SELECT
    o.customer_id,
    o.status,
    o.channel,
    s.shipped_date,
    s.carrier
FROM orders o
LEFT JOIN shipments s ON s.order_id = o.id
WHERE o.amount <= 700
