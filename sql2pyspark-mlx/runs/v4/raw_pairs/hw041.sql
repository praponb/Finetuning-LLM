SELECT id, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rank_num FROM employees;
