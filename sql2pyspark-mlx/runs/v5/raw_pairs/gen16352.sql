SELECT
    a.id,
    a.account_type,
    a.status,
    a.customer_id,
    c.city
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id;
