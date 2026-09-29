SELECT c.country, c.city, COUNT(CASE WHEN a.status = 'OPEN' THEN 1 END) AS open_count
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id
GROUP BY c.country, c.city
HAVING COUNT(CASE WHEN a.status = 'OPEN' THEN 1 END) >= 4
ORDER BY city, open_count DESC, country
LIMIT 50
