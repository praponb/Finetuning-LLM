SELECT id, job_title, TRIM(level) AS clean_level
FROM employees
ORDER BY job_title, id
LIMIT 10;
