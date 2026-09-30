SELECT s.order_id, o.channel
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id;
