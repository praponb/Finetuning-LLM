SELECT
    manager_id,
    job_title,
    SUM(CASE WHEN job_title = 'Analyst' THEN salary ELSE 0 END) AS analyst_salary,
    ROUND((100 * SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_manager,
    MAX(salary) AS max_salary
FROM employees
WHERE manager_id IN (8, 17, 19, 26)
GROUP BY 1, 2;
