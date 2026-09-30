SELECT status, SUM(discount) AS total_discount
FROM orders
GROUP BY status
HAVING SUM(discount) > 1.7
