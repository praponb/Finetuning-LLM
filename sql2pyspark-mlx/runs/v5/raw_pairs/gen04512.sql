SELECT
    s.id,
    s.browser,
    s.customer_id,
    c.email,
    c.name,
    c.credit_limit
FROM sessions s
RIGHT OUTER JOIN customers c ON c.id = s.customer_id
ORDER BY customer_id DESC;
