SELECT
    a.id,
    a.opened_date,
    a.account_type,
    c.email,
    c.city,
    c.credit_limit
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.status IN ('OPEN', 'FROZEN') AND c.age <= 34;
