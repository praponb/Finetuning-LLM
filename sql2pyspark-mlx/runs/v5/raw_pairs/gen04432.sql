SELECT
    s.customer_id,
    s.started_at,
    c.segment,
    c.signup_date
FROM sessions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IS NOT NULL;
