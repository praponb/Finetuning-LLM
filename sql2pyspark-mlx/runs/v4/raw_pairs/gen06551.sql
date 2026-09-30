SELECT id, dept_id, SUBSTRING(email, 1, 4) AS email_prefix
FROM employees
WHERE job_title IN ('Director', 'Designer') AND (dept_id IN (14, 16) OR NOT is_remote)
