SELECT is_remote, job_title, analyst_salary
FROM (
    SELECT is_remote, job_title, SUM(CASE WHEN job_title = 'Analyst' THEN salary ELSE 0 END) AS analyst_salary
    FROM employees
    WHERE dept_id IN (16, 19, 33)
    GROUP BY is_remote, job_title
) AS s
WHERE analyst_salary > 48
ORDER BY analyst_salary DESC;
