SELECT status
FROM orders
WHERE order_date >= '2025-06-01'
INTERSECT
SELECT status
FROM payments;
