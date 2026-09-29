SELECT c.country, COUNT(o.id) AS order_count
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
WHERE c.age > 37
GROUP BY c.country
ORDER BY c.country;
