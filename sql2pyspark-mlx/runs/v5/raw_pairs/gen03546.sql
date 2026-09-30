SELECT job_title
FROM employees
WHERE is_remote
EXCEPT
SELECT job_title
FROM employees
WHERE is_remote = FALSE
ORDER BY job_title DESC
