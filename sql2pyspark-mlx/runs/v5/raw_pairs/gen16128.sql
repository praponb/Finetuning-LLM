SELECT
    a.balance,
    c.credit_limit,
    c.city,
    c.is_active
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE a.opened_date <= '2024-05-01' AND LENGTH(c.name) < 12
ORDER BY city DESC, 2
