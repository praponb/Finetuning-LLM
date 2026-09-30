SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY hire_date DESC, id) AS row_num FROM employees QUALIFY row_num = 1;
