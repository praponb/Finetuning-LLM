SELECT level, SUM(salary) total_salary
FROM employees
GROUP BY level
ORDER BY total_salary DESC
