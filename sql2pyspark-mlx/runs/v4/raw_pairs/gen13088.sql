SELECT
    s.id,
    s.browser,
    s.customer_id,
    s.started_at,
    c.age,
    c.signup_date,
    c.credit_limit
FROM sessions s
RIGHT JOIN customers c ON s.customer_id = c.id;
