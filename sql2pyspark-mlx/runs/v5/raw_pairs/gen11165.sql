SELECT id, email, dept_id, manager_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS row_num FROM employees) x WHERE row_num = 1
