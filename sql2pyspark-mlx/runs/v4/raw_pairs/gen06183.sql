SELECT
    s.country,
    s.browser,
    s.device,
    c.name,
    c.email,
    c.signup_date,
    DATE_TRUNC('year', c.signup_date) AS signup_date_period
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id;
