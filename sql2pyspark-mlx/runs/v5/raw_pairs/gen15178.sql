SELECT o.status, o.channel, COUNT(*) AS total_count
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
GROUP BY o.status, o.channel
ORDER BY o.channel, o.status;
