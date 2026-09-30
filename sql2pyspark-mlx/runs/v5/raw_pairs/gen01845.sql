SELECT s.id, s.carrier, s.status, s.cost FROM shipments s WHERE s.carrier = 'UPS' AND s.order_id NOT IN (SELECT id FROM orders WHERE store_id = 27)
