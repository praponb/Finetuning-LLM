SELECT DATE_TRUNC('MONTH', c.signup_date) AS signup_month, ROUND((100 * SUM(CASE WHEN o.channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
GROUP BY DATE_TRUNC('MONTH', c.signup_date)
ORDER BY pct_web DESC, signup_month DESC;
