SELECT c.is_active, ROUND((100 * SUM(CASE WHEN s.status = 'ACTIVE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_active, ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active
HAVING pct_active > 61
ORDER BY avg_monthly_fee DESC, is_active
LIMIT 50;
