SELECT
    CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END end_cohort,
    MAX(monthly_fee) max_monthly_fee,
    AVG(monthly_fee) avg_monthly_fee,
    ROUND((100 * SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_active
FROM subscriptions
GROUP BY end_cohort
ORDER BY avg_monthly_fee DESC
