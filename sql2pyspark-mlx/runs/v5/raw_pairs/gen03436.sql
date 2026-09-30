SELECT s.id, s.status, s.carrier, s.delivered_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE YEAR(order_date) >= 2024);
