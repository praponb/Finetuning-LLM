SELECT o.id, o.status, o.channel
FROM orders o
WHERE o.discount BETWEEN 0.14 AND 0.39 AND o.id IN (SELECT order_id FROM shipments)
ORDER BY status ASC, o.channel ASC;
