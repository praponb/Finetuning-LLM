SELECT
    p.id,
    p.amount,
    p.method,
    p.order_id
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE customer_id IS NOT NULL)
ORDER BY method
