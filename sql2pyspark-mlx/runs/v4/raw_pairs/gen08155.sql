SELECT
    s.id,
    s.plan,
    c.signup_date,
    ADD_MONTHS(s.start_date, 6) AS start_date_plus_months
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id;
