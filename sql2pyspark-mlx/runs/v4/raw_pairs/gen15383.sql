SELECT s.status, o.order_date FROM shipments s INNER JOIN orders o ON o.id = s.order_id ORDER BY order_date DESC;
