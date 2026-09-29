SELECT s.id, s.order_id, o.customer_id FROM shipments s LEFT JOIN orders o ON s.order_id = o.id WHERE o.discount > 0.33 OR o.order_date >= '2024-12-01' ORDER BY s.id ASC;
