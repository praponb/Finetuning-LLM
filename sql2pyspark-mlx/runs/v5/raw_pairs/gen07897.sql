SELECT
    e.*,
    p.email,
    p.name
FROM accounts e
LEFT JOIN customers p
    ON e.customer_id = p.id
