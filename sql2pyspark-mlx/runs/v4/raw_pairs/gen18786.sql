SELECT d.id, d.location, d.dept_name, t.max_salary FROM departments d LEFT OUTER JOIN (SELECT dept_id, MAX(salary) AS max_salary FROM employees GROUP BY dept_id) t ON d.id = t.dept_id;
