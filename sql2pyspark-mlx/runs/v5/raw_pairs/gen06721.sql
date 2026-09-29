SELECT
    p.city,
    p.country,
    COUNT(*) AS rows_total,
    MAX(s.balance) AS max_balance
FROM accounts s
INNER JOIN customers p ON s.customer_id = p.id
GROUP BY p.city, p.country
ORDER BY country;
