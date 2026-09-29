SELECT
    o.id,
    o.order_date,
    o.channel,
    o.customer_id
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id);
