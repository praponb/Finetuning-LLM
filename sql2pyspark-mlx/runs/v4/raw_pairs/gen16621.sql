SELECT s.status, s.cost, s.order_id, o.customer_id FROM shipments s INNER JOIN orders o ON s.order_id = o.id;
