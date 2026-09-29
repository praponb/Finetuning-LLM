SELECT plan, status, ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee
FROM subscriptions
GROUP BY plan, status
HAVING AVG(monthly_fee) >= 240
ORDER BY plan DESC
