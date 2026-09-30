SELECT s.id, s.shipped_date, s.status, s.carrier FROM shipments s WHERE s.delivered_date <= '2023-04-01' AND s.order_id IN (SELECT id FROM orders WHERE store_id = 24) ORDER BY s.carrier ASC
