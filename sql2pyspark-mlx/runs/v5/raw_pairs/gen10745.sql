SELECT shipments.id, shipments.status, shipments.order_id, orders.status AS order_status, orders.order_date, orders.customer_id FROM shipments JOIN orders ON shipments.order_id = orders.id
