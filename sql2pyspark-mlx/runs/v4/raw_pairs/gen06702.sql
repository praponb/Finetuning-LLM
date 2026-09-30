SELECT
    customer_id,
    CASE WHEN monthly_fee >= 182 THEN 'high' WHEN monthly_fee >= 77 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    status,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY customer_id, monthly_fee_bucket, status
HAVING ROUND(AVG(monthly_fee), 2) >= 27
ORDER BY avg_monthly_fee DESC
