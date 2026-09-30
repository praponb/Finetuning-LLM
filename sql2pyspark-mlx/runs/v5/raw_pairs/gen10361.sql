SELECT level, ROUND(AVG(salary), 2) AS avg_salary, MIN(salary) AS min_salary
FROM employees
WHERE job_title IS NOT NULL OR job_title = 'Designer'
GROUP BY level
ORDER BY avg_salary;
