SELECT plan, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled
FROM subscriptions
WHERE start_date <= '2024-01-01'
GROUP BY plan;
