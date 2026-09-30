SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer
FROM employees
WHERE job_title IS NULL
GROUP BY hire_month
ORDER BY hire_month ASC, pct_designer ASC
