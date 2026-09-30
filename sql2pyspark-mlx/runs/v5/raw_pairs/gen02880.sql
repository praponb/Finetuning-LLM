SELECT o.id, o.channel, o.amount
FROM orders o
WHERE o.amount < 590 AND o.id IN (SELECT order_id FROM shipments WHERE MONTH(shipped_date) IN (3, 10, 12))
