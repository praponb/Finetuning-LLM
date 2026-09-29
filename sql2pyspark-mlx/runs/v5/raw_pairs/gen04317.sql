SELECT
    customer_id,
    CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort,
    COUNT(DISTINCT monthly_fee) AS unique_statuss,
    MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
GROUP BY 1, 2
ORDER BY unique_statuss ASC
