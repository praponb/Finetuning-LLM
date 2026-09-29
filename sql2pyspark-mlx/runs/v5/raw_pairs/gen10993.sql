SELECT CASE WHEN monthly_fee >= 227 THEN 'high' WHEN monthly_fee >= 115 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, SUM(monthly_fee) AS total_monthly_fee, SUM(SUM(monthly_fee)) OVER (ORDER BY CASE WHEN monthly_fee >= 227 THEN 'high' WHEN monthly_fee >= 115 THEN 'mid' ELSE 'low' END) AS cumulative_monthly_fee
FROM subscriptions
WHERE plan = 'basic'
GROUP BY CASE WHEN monthly_fee >= 227 THEN 'high' WHEN monthly_fee >= 115 THEN 'mid' ELSE 'low' END
