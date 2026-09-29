SELECT s.id, s.cost
FROM shipments s
WHERE s.carrier <> 'Flash' AND s.order_id NOT IN (SELECT id FROM orders WHERE status = 'RETURNED');
