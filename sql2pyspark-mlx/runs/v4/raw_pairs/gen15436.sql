SELECT CASE WHEN credit_limit >= 17800 THEN 'high' WHEN credit_limit >= 4200 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, COUNT(DISTINCT age) AS unique_countrys
FROM customers
GROUP BY CASE WHEN credit_limit >= 17800 THEN 'high' WHEN credit_limit >= 4200 THEN 'mid' ELSE 'low' END, CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING unique_countrys < 3;
