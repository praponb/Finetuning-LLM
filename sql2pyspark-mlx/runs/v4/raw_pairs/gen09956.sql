SELECT QUARTER(order_date) order_quarter, COUNT(discount) discount_count
FROM orders
WHERE status <> 'PENDING'
GROUP BY QUARTER(order_date)
HAVING AVG(discount) >= 0.34
ORDER BY discount_count DESC
