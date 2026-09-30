SELECT s.id, s.status
FROM shipments s
WHERE s.shipped_date < '2025-04-01' AND s.order_id IN (SELECT id FROM orders WHERE channel <> 'partner');
