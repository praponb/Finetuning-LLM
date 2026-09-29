SELECT products.category, AVG(order_items.quantity) AS avg_quantity
FROM order_items
LEFT JOIN products ON order_items.product_id = products.id
LEFT JOIN orders ON order_items.order_id = orders.id
GROUP BY products.category
HAVING AVG(order_items.quantity) > 7.1
