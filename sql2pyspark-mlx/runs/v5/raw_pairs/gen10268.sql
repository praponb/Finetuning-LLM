SELECT s.id, s.shipped_date, s.order_id FROM shipments s WHERE s.carrier NOT IN ('DHL', 'FedEx') AND s.order_id IN (SELECT id FROM orders WHERE amount < 640) ORDER BY s.shipped_date
