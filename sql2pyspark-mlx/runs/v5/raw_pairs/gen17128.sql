SELECT a.id, a.dept_id, a.salary FROM employees a WHERE a.salary = (SELECT MIN(b.salary) FROM employees b WHERE b.dept_id = a.dept_id) ORDER BY a.salary;
