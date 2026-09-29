SELECT o.id, o.channel, COUNT(oi.id) AS order_item_count
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
GROUP BY o.id, o.channel;
