SELECT
    a.id,
    a.account_type,
    c.email,
    c.city
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.balance >= 48000
ORDER BY a.id DESC
LIMIT 1;
