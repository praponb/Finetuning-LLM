SELECT dept_id, level
FROM employees
WHERE salary BETWEEN 81000 AND 139000
UNION
SELECT dept_id, level
FROM employees
WHERE job_title IS NOT NULL
