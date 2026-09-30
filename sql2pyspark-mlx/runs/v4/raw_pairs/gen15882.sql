SELECT orders.customer_id, orders.channel, MAX(order_items.quantity) AS max_quantity
FROM order_items
LEFT OUTER JOIN orders ON orders.id = order_items.order_id
GROUP BY orders.customer_id, orders.channel
ORDER BY customer_id;
