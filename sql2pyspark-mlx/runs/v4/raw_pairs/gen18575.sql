SELECT s.city, MIN(o.order_date) AS first_order_date
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.status IN ('CANCELLED', 'COMPLETED', 'RETURNED')
GROUP BY s.city
