SELECT is_remote, COUNT(DISTINCT salary) AS unique_job_titles, MIN(hire_date) AS first_hire_date
FROM employees
WHERE is_remote AND job_title IN ('Intern', 'Designer', 'Director')
GROUP BY 1;
