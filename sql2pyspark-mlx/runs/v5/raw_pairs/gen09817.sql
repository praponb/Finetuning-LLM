SELECT id, dept_id, hire_date FROM (SELECT id, dept_id, hire_date, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS pos FROM employees) AS ranked WHERE pos <= 3 ORDER BY 3
