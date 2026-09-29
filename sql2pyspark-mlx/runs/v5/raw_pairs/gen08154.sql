SELECT plan, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE status = 'EXPIRED' OR start_date < '2023-03-31'
GROUP BY plan
ORDER BY avg_monthly_fee DESC, plan
LIMIT 3
