SELECT
    a.status,
    c.country,
    c.segment,
    c.city,
    c.age - c.credit_limit AS age_minus_credit_limit
FROM accounts a
RIGHT OUTER JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id IS NOT NULL
ORDER BY age_minus_credit_limit DESC;
