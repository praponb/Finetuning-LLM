SELECT
    c.id,
    c.country,
    c.email,
    c.age
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.customer_id IS NOT NULL);
