SELECT id, cost FROM shipments WHERE (status = 'IN_TRANSIT' AND order_id IN (9, 25, 39, 40)) OR cost >= 50;
