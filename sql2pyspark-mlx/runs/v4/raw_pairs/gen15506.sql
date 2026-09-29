SELECT
    c.id,
    c.country,
    c.credit_limit,
    c.city
FROM customers c
WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.status = 'CLOSED')
ORDER BY id DESC;
