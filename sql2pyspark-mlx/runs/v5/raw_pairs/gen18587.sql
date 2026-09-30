SELECT customer_id, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled, ROUND((100 * SUM(CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_enterprise
FROM subscriptions
WHERE customer_id = 24
GROUP BY 1
ORDER BY customer_id, pct_enterprise;
