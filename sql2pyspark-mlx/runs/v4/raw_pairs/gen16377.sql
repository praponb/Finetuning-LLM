SELECT level, max_salary, pct_designer
FROM (
    SELECT level, MAX(salary) AS max_salary, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer
    FROM employees
    WHERE full_name LIKE '%Kany%'
    GROUP BY level
) AS t
WHERE pct_designer >= 45
ORDER BY pct_designer DESC
