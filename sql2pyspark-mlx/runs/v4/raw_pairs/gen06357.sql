SELECT orders.channel, COUNT(order_items.id) AS order_item_count, AVG(order_items.unit_price) AS avg_unit_price
FROM orders
JOIN order_items ON order_items.order_id = orders.id
WHERE orders.status = 'RETURNED'
GROUP BY orders.channel
ORDER BY 1 ASC;
