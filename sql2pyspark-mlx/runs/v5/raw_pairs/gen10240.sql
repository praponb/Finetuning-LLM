SELECT MONTH(c.signup_date) AS signup_month, COUNT(CASE WHEN s.status = 'CANCELLED' THEN 1 END) AS cancelled_count, COUNT(*) AS num_subscriptions
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY MONTH(c.signup_date)
ORDER BY cancelled_count DESC
