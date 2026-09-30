SELECT d.id, d.dept_name, t.total_salary FROM departments d JOIN (SELECT dept_id, SUM(salary) total_salary FROM employees GROUP BY dept_id) t ON d.id = t.dept_id WHERE t.total_salary > 505000
