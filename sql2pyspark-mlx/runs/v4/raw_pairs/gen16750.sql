SELECT
    c.country,
    c.is_active,
    SUM(t.amount) AS total_amount,
    COUNT(*) AS cnt
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY c.country, c.is_active
HAVING SUM(t.amount) > 13000
ORDER BY country;
