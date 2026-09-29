SELECT c.country, o.customer_id
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
WHERE o.discount >= 0.22 AND LOWER(c.name) LIKE '%mma%'
ORDER BY c.country ASC, customer_id ASC
