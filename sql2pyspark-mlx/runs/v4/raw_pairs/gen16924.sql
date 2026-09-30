SELECT
    CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
    is_active,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    MIN(age) AS min_age
FROM customers
GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END, is_active
HAVING SUM(age) > 160
