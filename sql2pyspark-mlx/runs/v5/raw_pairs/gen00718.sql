SELECT end_cohort, max_monthly_fee, basic_monthly_fee
FROM (
    SELECT CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort, MAX(monthly_fee) AS max_monthly_fee, SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee
    FROM subscriptions
    GROUP BY CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS x
WHERE max_monthly_fee < 190
ORDER BY max_monthly_fee DESC, end_cohort
LIMIT 5;
