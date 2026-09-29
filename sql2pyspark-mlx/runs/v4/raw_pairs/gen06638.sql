SELECT c.id, c.city, COUNT(a.id) account_count
FROM customers c
LEFT OUTER JOIN accounts a ON a.customer_id = c.id
WHERE a.status = 'OPEN'
GROUP BY c.id, c.city
ORDER BY id, account_count DESC, city
LIMIT 3;
