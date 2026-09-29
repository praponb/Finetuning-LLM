SELECT shipments.shipped_date, shipments.order_id, orders.status FROM shipments INNER JOIN orders ON shipments.order_id = orders.id;
