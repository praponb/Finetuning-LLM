SELECT id, dept_id, level
FROM employees
WHERE is_remote AND hire_date >= '2024-04-01' AND job_title = 'Manager'
ORDER BY id;
