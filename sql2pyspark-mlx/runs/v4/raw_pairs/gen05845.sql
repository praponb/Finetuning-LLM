SELECT MONTH(hire_date) AS hire_month, job_title, MAX(salary) AS max_salary
FROM employees
WHERE job_title NOT IN ('Designer', 'Engineer') AND job_title IS NOT NULL AND job_title IS NOT NULL
GROUP BY 1, 2;
