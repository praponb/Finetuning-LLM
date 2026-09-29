SELECT d.dept_name, SUM(CASE WHEN e.level = 'L3' THEN e.salary ELSE 0 END) AS l3_salary FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.salary > 130000 GROUP BY d.dept_name;
