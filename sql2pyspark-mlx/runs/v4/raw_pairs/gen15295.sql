SELECT job_title, MIN(hire_date) AS min_hire_date
FROM employees
GROUP BY 1
ORDER BY 1, min_hire_date DESC
LIMIT 10
