SELECT a.id, a.dept_id, a.salary, a.full_name FROM employees a WHERE a.salary = (SELECT MAX(b.salary) FROM employees b WHERE b.dept_id = a.dept_id) ORDER BY a.salary DESC
