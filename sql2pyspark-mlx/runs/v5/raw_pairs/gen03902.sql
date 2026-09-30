SELECT orders.order_date, shipments.cost
FROM orders
LEFT JOIN shipments ON shipments.order_id = orders.id
ORDER BY orders.order_date
