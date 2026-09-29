SELECT s.status, c.signup_date, c.city
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.plan IN ('basic', 'premium') OR s.end_date < '2025-10-13'
