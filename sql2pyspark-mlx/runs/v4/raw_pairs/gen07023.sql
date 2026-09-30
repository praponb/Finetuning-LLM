SELECT
    s.monthly_fee,
    s.start_date,
    c.country,
    c.is_active,
    c.signup_date
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id;
