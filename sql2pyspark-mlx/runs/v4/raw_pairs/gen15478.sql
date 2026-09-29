SELECT s.id, s.order_id, s.cost, o.order_date FROM shipments s INNER JOIN orders o ON s.order_id = o.id
