SELECT id, dept_name, COUNT(*) OVER (PARTITION BY dept_name) AS dept_name_count FROM departments;
