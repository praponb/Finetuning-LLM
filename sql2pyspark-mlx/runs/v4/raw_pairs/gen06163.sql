SELECT ROUND(AVG(salary), 2) avg_salary, MAX(salary) - MIN(salary) salary_range FROM employees WHERE manager_id = 9
