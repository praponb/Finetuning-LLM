SELECT
    c.id,
    c.credit_limit,
    c.country,
    t.min_balance,
    t.open_count
FROM customers c
LEFT JOIN (SELECT customer_id, MIN(balance) AS min_balance, COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.open_count >= 9
ORDER BY c.country;
