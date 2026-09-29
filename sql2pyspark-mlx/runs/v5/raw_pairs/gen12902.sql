SELECT job_title, COUNT(*) AS total_count, ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
GROUP BY 1
ORDER BY stddev_salary;
