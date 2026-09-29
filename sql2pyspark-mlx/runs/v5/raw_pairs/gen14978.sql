SELECT
    c.segment,
    c.signup_date,
    c.country,
    s.plan
FROM customers c
LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id;
