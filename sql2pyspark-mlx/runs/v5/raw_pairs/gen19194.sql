SELECT CASE WHEN monthly_fee >= 207 THEN 'high' WHEN monthly_fee >= 39 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM subscriptions
GROUP BY CASE WHEN monthly_fee >= 207 THEN 'high' WHEN monthly_fee >= 39 THEN 'mid' ELSE 'low' END
ORDER BY monthly_fee_bucket
