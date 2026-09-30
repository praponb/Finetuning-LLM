SELECT
    plan,
    CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort,
    MAX(monthly_fee) AS max_monthly_fee,
    COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count,
    MIN(start_date) AS min_start_date
FROM subscriptions
GROUP BY plan, end_cohort
HAVING MAX(monthly_fee) < 160;
