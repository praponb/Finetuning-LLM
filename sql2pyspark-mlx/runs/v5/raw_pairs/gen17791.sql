SELECT c.city, COUNT(t.amount) AS amount_count, COUNT(DISTINCT t.amount) AS unique_merchants
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON c.id = a.customer_id
WHERE NOT c.is_active
GROUP BY c.city
ORDER BY unique_merchants DESC, city
LIMIT 1;
