SELECT status, COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count
FROM subscriptions
GROUP BY status
HAVING SUM(monthly_fee) < 1150
ORDER BY status
