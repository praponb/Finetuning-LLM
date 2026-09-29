SELECT c.id, c.segment
FROM customers c
WHERE c.city <> 'Tokyo' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.customer_id IN (11, 17, 20));
