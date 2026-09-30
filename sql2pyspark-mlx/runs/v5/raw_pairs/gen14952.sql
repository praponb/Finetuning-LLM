SELECT CASE WHEN segment IN ('Consumer', 'SMB') THEN 'priority' ELSE 'other' END segment_group, CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END signup_cohort, SUM(age) total_age
FROM customers
GROUP BY segment_group, signup_cohort
HAVING SUM(age) < 360
ORDER BY segment_group, signup_cohort
LIMIT 50;
