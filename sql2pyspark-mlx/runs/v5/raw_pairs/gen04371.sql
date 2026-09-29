SELECT orders.customer_id, ROUND(STDDEV(order_items.quantity), 2) AS stddev_quantity
FROM order_items
JOIN orders ON order_items.order_id = orders.id
JOIN stores ON stores.id = orders.store_id
GROUP BY orders.customer_id
ORDER BY stddev_quantity DESC
