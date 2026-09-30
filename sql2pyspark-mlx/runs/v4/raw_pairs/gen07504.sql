SELECT
    a.customer_id,
    a.balance,
    c.is_active,
    c.signup_date,
    c.credit_limit
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.signup_date <= '2025-07-03'
ORDER BY credit_limit DESC
