SELECT orders.customer_id, shipments.delivered_date, shipments.status, shipments.carrier FROM orders LEFT JOIN shipments ON shipments.order_id = orders.id ORDER BY carrier
