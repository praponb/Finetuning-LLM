SELECT s.region, COUNT(DISTINCT o.amount) AS unique_customers
FROM orders o
LEFT OUTER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY s.region
ORDER BY unique_customers, s.region;
