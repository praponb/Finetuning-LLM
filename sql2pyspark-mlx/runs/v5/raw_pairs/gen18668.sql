SELECT
    dept_id,
    is_remote,
    COUNT(DISTINCT salary) AS unique_job_titles,
    ROUND(STDDEV(salary), 2) AS stddev_salary,
    MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE NOT is_remote AND is_remote = FALSE
GROUP BY 1, 2
HAVING COUNT(DISTINCT salary) > 4
ORDER BY unique_job_titles, 5;
