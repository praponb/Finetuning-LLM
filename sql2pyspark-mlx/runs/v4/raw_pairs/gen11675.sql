SELECT manager_id, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY manager_id
HAVING ROUND(AVG(salary), 2) >= 76;
