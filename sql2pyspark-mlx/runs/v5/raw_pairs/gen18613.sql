SELECT
    c.id,
    c.email,
    COUNT(s.id) AS subscription_count,
    ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee
FROM customers c
LEFT JOIN subscriptions s ON s.customer_id = c.id
GROUP BY c.id, c.email
HAVING ROUND(AVG(s.monthly_fee), 2) > 56
ORDER BY c.email
