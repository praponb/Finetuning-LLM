SELECT shipments.id, shipments.cost, shipments.status, orders.amount, orders.status AS order_status FROM shipments INNER JOIN orders ON shipments.order_id = orders.id;
