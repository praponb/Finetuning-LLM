SELECT p.id, p.method, p.status
FROM payments p
WHERE p.order_id IN (12, 31) AND p.order_id NOT IN (SELECT id FROM orders WHERE order_date < '2025-03-01');
