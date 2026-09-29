SELECT status, customer_id
FROM orders
WHERE status = 'CANCELLED'
UNION
SELECT status, customer_id
FROM orders
WHERE amount >= 910
