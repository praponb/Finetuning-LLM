SELECT
    s.end_date,
    s.monthly_fee,
    c.city,
    c.age - c.credit_limit AS age_minus_credit_limit
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit <= 13100;
