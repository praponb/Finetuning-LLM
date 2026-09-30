SELECT s.id, s.carrier, s.cost, s.order_id, o.store_id FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE o.customer_id = 11;
