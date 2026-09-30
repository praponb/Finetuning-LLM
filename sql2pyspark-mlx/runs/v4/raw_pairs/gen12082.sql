SELECT
    job_title,
    CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort,
    is_remote,
    ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4
FROM employees
WHERE level = 'L4'
GROUP BY 1, 2, 3
ORDER BY 1
