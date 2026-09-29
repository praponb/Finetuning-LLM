SELECT o.store_id, o.channel, SUM(oi.unit_price) AS total_unit_price
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
GROUP BY o.store_id, o.channel;
