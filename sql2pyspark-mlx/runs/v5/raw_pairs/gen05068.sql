SELECT *
FROM (
    SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, is_remote, ROUND((100 * SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_analyst
    FROM employees
    WHERE manager_id IS NULL
    GROUP BY CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END, is_remote
) AS sub
WHERE pct_analyst < 20
ORDER BY pct_analyst DESC
