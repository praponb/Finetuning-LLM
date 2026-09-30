SELECT DATE_TRUNC('MONTH', c.signup_date) AS signup_month, ROUND((100 * SUM(CASE WHEN s.status = 'EXPIRED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_expired
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
GROUP BY DATE_TRUNC('MONTH', c.signup_date)
ORDER BY pct_expired DESC
