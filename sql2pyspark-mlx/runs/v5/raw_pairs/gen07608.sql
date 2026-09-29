SELECT
    a.balance,
    a.customer_id,
    a.account_type,
    c.city,
    c.signup_date
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
ORDER BY account_type, city DESC
