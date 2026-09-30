SELECT
    c.id,
    c.city,
    c.credit_limit,
    c.age
FROM customers c
WHERE c.id IN (SELECT customer_id FROM accounts);
