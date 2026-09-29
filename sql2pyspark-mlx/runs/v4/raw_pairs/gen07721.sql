SELECT CASE WHEN monthly_fee >= 252 THEN 'high' WHEN monthly_fee >= 120 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, SORT_ARRAY(COLLECT_SET(plan)) AS plan_list, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
FROM subscriptions
GROUP BY CASE WHEN monthly_fee >= 252 THEN 'high' WHEN monthly_fee >= 120 THEN 'mid' ELSE 'low' END
HAVING COUNT(DISTINCT monthly_fee) < 4;
