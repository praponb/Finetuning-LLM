SELECT o.id, o.channel
FROM orders o
WHERE o.amount > 780 AND o.id IN (SELECT order_id FROM shipments WHERE cost > 36);
