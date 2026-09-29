SELECT s.id, s.status, s.carrier FROM shipments s WHERE s.carrier IN ('FedEx', 'UPS', 'Kerry') AND s.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.14 AND 0.34);
