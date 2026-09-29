SELECT orders.status, shipments.delivered_date, shipments.order_id
FROM orders
INNER JOIN shipments ON shipments.order_id = orders.id;
