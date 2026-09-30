SELECT
    DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month,
    c.city,
    COUNT(CASE WHEN s.plan = 'premium' THEN 1 END) AS premium_count,
    SUM(CASE WHEN s.plan = 'enterprise' THEN 1 ELSE 0 END) AS enterprise_count
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.end_date > '2024-11-01'
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM'), c.city
ORDER BY 1, city
LIMIT 50;
