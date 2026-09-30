SELECT
    a.customer_id,
    a.status,
    a.opened_date,
    c.country,
    c.email
FROM accounts a
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE UPPER(c.email) LIKE '%MMA%';
