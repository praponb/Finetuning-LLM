SELECT
    CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
    country,
    AVG(age) AS avg_age,
    MAX(signup_date) AS last_signup_date
FROM customers
WHERE credit_limit >= 15800
GROUP BY CUBE(CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END, country);
