SELECT end_quarter, monthly_fee_bucket, stddev_monthly_fee
FROM (
    SELECT QUARTER(end_date) AS end_quarter, CASE WHEN monthly_fee >= 234 THEN 'high' WHEN monthly_fee >= 76 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM subscriptions
    WHERE end_date >= '2023-11-16'
    GROUP BY QUARTER(end_date), CASE WHEN monthly_fee >= 234 THEN 'high' WHEN monthly_fee >= 76 THEN 'mid' ELSE 'low' END
) AS x
WHERE stddev_monthly_fee >= 49
ORDER BY stddev_monthly_fee;
