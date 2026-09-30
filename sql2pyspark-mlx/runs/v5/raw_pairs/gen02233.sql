SELECT CASE WHEN monthly_fee >= 225 THEN 'high' WHEN monthly_fee >= 83 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, SUM(monthly_fee) AS total_monthly_fee, SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee ELSE 0 END) AS active_monthly_fee
FROM subscriptions
GROUP BY monthly_fee_bucket;
