SELECT
    t.merchant,
    a.customer_id,
    a.account_type,
    c.signup_date
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.balance < 37000
ORDER BY customer_id DESC
