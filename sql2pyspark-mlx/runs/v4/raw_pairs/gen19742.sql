SELECT c.id, c.email, COUNT(a.id) AS account_count
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
GROUP BY c.id, c.email
HAVING COUNT(a.id) >= 1
ORDER BY email ASC;
