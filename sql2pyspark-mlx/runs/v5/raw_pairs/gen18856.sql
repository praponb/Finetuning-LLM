SELECT SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) AS manager_count
FROM employees
WHERE dept_id IN (12, 14, 38)
ORDER BY manager_count DESC;
