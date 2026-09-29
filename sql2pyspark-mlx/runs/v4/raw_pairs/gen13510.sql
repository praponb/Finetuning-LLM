SELECT plan, status, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
FROM subscriptions
GROUP BY plan, status
ORDER BY monthly_fee_range DESC
