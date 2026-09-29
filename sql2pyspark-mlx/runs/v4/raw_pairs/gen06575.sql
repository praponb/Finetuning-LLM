SELECT status, AVG(monthly_fee) avg_monthly_fee
FROM subscriptions
GROUP BY status
ORDER BY status
