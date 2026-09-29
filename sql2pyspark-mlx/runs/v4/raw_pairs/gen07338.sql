SELECT
    s.monthly_fee,
    s.end_date,
    c.country,
    c.is_active,
    c.signup_date,
    TRIM(c.country) AS clean_country
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
WHERE s.monthly_fee < 150
