SELECT status
FROM orders
WHERE YEAR(order_date) >= 2025
INTERSECT
SELECT status
FROM orders
WHERE order_date BETWEEN '2023-07-01' AND '2025-08-24'
ORDER BY status;
