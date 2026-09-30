SELECT c.city, COUNT(CASE WHEN t.merchant = 'Lazada' THEN 1 END) AS lazada_count, AVG(t.amount) AS avg_amount
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
JOIN customers c ON c.id = a.customer_id
WHERE c.signup_date >= '2024-12-09'
GROUP BY c.city
HAVING AVG(t.amount) > 3900
ORDER BY avg_amount DESC, city
LIMIT 3
