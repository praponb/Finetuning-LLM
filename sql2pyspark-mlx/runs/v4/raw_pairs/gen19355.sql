SELECT
    s.id,
    s.start_date,
    s.monthly_fee,
    c.country,
    c.segment
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.email IS NOT NULL
ORDER BY s.id DESC;
