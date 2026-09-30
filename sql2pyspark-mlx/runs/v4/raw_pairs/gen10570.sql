SELECT orders.id, COUNT(order_items.id) AS order_item_count, COUNT(order_items.quantity) AS quantity_count
FROM orders
LEFT JOIN order_items ON orders.id = order_items.order_id
WHERE orders.status = 'CANCELLED'
GROUP BY orders.id
HAVING order_item_count > 2
ORDER BY id DESC
LIMIT 3
