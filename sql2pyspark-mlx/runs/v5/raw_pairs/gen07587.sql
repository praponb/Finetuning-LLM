SELECT
    CASE WHEN salary >= 152000 THEN 'high' WHEN salary >= 69000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
    SUM(salary) AS total_salary,
    ROUND((100 * SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l2
FROM employees
WHERE dept_id = 13
GROUP BY salary_bucket, hire_cohort
