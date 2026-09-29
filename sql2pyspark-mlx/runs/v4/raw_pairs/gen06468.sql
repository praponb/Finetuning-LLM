SELECT *, DATE_TRUNC('MONTH', hire_date) AS hire_date_period, CONCAT(level, '/', job_title) AS level_job_title
FROM employees
ORDER BY id DESC
