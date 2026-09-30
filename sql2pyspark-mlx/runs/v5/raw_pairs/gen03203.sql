SELECT COUNT(DISTINCT salary) AS unique_job_titles, MIN(salary) AS min_salary, COUNT(DISTINCT salary) AS unique_managers
FROM employees
ORDER BY unique_job_titles
