SELECT level, hire_date
FROM employees
WHERE YEAR(hire_date) >= 2024
UNION
SELECT level, hire_date
FROM employees
WHERE job_title != 'Designer'
ORDER BY hire_date;
