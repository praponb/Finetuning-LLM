SELECT customer_id, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY 1
ORDER BY avg_monthly_fee DESC, customer_id
LIMIT 3;
