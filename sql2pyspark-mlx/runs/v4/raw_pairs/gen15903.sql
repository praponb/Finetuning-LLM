SELECT salary, CONCAT_WS('/', email, job_title) AS email_job_title_key
FROM employees
WHERE job_title IN ('Engineer', 'Designer')
