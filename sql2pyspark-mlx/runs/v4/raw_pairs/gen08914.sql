SELECT s.id, s.cost FROM shipments s WHERE s.shipped_date <= '2023-07-01' AND s.order_id NOT IN (SELECT id FROM orders WHERE store_id IS NOT NULL);
