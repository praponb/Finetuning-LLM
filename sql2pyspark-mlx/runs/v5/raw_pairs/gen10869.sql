SELECT c.city, c.country, ROUND(AVG(a.balance), 2) AS avg_balance
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id IS NOT NULL
GROUP BY c.city, c.country
HAVING ROUND(AVG(a.balance), 2) >= 79
ORDER BY city ASC
