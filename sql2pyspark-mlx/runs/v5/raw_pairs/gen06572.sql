SELECT is_remote, job_title
FROM employees
WHERE UPPER(email) LIKE '%MMA%' AND LENGTH(full_name) > 10;
