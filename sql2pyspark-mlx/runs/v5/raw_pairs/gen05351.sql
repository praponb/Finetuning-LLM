SELECT id, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) salary_row FROM employees WHERE dept_id IN (1, 11, 23, 34) ORDER BY dept_id DESC, salary DESC
