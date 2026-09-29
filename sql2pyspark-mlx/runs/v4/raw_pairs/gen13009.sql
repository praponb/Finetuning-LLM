SELECT *
FROM (
    SELECT CASE WHEN plan IN ('standard', 'basic') THEN 'priority' ELSE 'other' END AS plan_group, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count
    FROM subscriptions
    GROUP BY CASE WHEN plan IN ('standard', 'basic') THEN 'priority' ELSE 'other' END
) AS agg
WHERE stddev_monthly_fee >= 40
