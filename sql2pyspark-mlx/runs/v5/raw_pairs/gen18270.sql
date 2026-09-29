SELECT is_remote, DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, MAX(hire_date) AS last_hire_date
FROM employees
WHERE is_remote = TRUE
GROUP BY 1, 2
