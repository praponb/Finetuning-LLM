SELECT
    CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
    SUM(age) AS total_age,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank
FROM customers
GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY signup_cohort, cnt DESC
