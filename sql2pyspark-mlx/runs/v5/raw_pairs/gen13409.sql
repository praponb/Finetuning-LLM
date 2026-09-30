SELECT
    a.account_type,
    a.status,
    a.customer_id,
    c.credit_limit,
    c.city,
    c.name
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE UPPER(c.name) LIKE '%RAC%'
