SELECT c.city, MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
WHERE c.signup_date >= '2025-10-01'
GROUP BY c.city
